import copy
from datetime import datetime, timedelta
from dateutil.parser import parse
from base import SnapchatBase
from tap_tester import runner, connections, menagerie

class SnapchatBookmarksTest(SnapchatBase):

    parent_child_mapping = {
        # child: parent
        'funding_sources': 'organization',
        'billing_centers': 'organization',
        'members': 'organization',
        'roles': 'organization',
        'ad_accounts': 'organization',
        'ad_account_stats_daily': 'ad_account',
        'ad_account_stats_hourly': 'ad_account',
        'audience_segments': 'ad_account',
        'pixels': 'ad_account',
        'pixel_domain_stats': 'pixel',
        'media': 'ad_account',
        'creatives': 'ad_account',
        'phone_numbers': 'ad_account',
        'campaigns': 'ad_account',
        'campaign_stats_daily': 'campaign',
        'campaign_stats_hourly': 'campaign',
        'ad_squads': 'ad_account',
        'ad_squad_stats_daily': 'ad_squad',
        'ad_squad_stats_hourly': 'ad_squad',
        'ads': 'ad_account',
        'ad_stats_daily': 'ad',
        'ad_stats_hourly': 'ad',
        'product_catalogs': 'organization',
        'product_sets': 'product_catalog'
    }

    def name(self):
        return 'tap_tester_snapchat_ads_bookmarks_test'

    def test_run(self):
        """
        Testing that the bookmarking for the tap works as expected
        - Verify for each incremental stream you can do a sync which records bookmarks
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark
        - All data of the 2nd sync is >= the bookmark from the first sync
        - The number of records in the 2nd sync is less then the first
        """

        self.START_DATE = "2020-01-01T00:00:00Z"
        conn_id = connections.ensure_connection(self, original_properties=False)
        runner.run_check_mode(self, conn_id)

        expected_streams = self.expected_streams() - self.stats_streams - self.missing_targeting_streams

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State
        ##########################################################################

        # setting 'second_start_date' as bookmark for running 2nd sync
        new_state = copy.deepcopy(first_sync_bookmarks)
        # add state for bookmark stored for streams in 1st sync
        for stream, bookmark_dict in first_sync_bookmarks.get('bookmarks').items():
            for key, value in bookmark_dict.items():
                # this checks if we have replicated any records for a parent id
                if value != self.START_DATE:
                    second_start_date = (datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    new_state['bookmarks'][stream][key] = second_start_date
                else:
                    new_state['bookmarks'][stream][key] = value

        # Set state for next sync
        menagerie.set_state(conn_id, new_state)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        for stream in expected_streams:

            with self.subTest(stream=stream):
                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_messages = [record.get('data') for record in first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']

                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if self.is_incremental(stream):

                    self.assertEqual(first_bookmark_key_value, second_bookmark_key_value)

                    # if the stream is child stream, then collect parent stream records for collecting ids
                    if self.parent_child_mapping.get(stream):
                        parent_records = [record.get('data') for record in first_sync_records.get(self.parent_child_mapping.get(stream) + 's').get('messages')
                                        if record.get('action') == 'upsert']

                    # NOTE: Sample bookmark:
                    #   "ad_accounts": {
                    #       "updated_at(parent_organization_id:organization-id-123)": "2020-01-01T00:00:00Z"
                    #   }
                    for key, value in first_bookmark_key_value.items():
                        # if the current stream is child stream, then we need to process bookmark for assertion
                        parent_stream = self.parent_child_mapping.get(stream)
                        if parent_stream:
                            # partial split
                            partial_splitted = key.split('parent_')
                            splitted = partial_splitted[1].split(':')
                            # parent value
                            parent_value = splitted[0]
                            # get parent record and remove last char ')' as we have splitted
                            parent_record = splitted[1][:-1]

                            # verify the parent in the bookmark is as expected
                            self.assertEqual(parent_value, parent_stream + '_id')

                            # collect parent ids
                            parent_ids = []
                            for parent_rec in parent_records:
                                parent_ids.append(parent_rec.get('id'))

                            # verify the id in the bookmark is present in the parent ids
                            self.assertIn(parent_record, parent_ids, f'The bookmark contains other \'id\': {parent_record} rather than parent\'s \'id\': {parent_ids}')

                            records_sync_1 = []
                            records_sync_2 = []

                            # collect records for a particular parent id from the stream for both syncs
                            for record in first_sync_messages:
                                if parent_record == record.get(f"{parent_stream}_id"):
                                    records_sync_1.append(record)
                            for record in second_sync_messages:
                                if parent_record == record.get(f"{parent_stream}_id"):
                                    records_sync_2.append(record)
                        else:
                            # as the stream is not child stream, no need to pre-process the records
                            records_sync_1 = first_sync_messages
                            records_sync_2 = second_sync_messages

                        bookmark_date_for_2nd_sync = new_state['bookmarks'][stream][key]

                        # validate records
                        self.validate_records(stream, records_sync_1, records_sync_2, value, bookmark_date_for_2nd_sync)
                else:
                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

    def validate_records(self, stream, records_sync_1, records_sync_2, state_file_bookmark, bookmark_date_for_2nd_sync):
        """
            Validate the records from both syncs with bookmark in the state file
            and verify we collected records from 1st state bookmark for 2nd sync
        """

        # collect information specific to incremental streams from syncs 1 & 2
        replication_key = next(iter(self.expected_replication_keys()[stream]))
        bookmark_value_parsed = parse(state_file_bookmark).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Verify the first sync sets a bookmark of the expected form
        self.assertIsNotNone(state_file_bookmark)

        for record in records_sync_2:

            # Verify the second sync bookmark value is the max replication key value for a given stream
            replication_key_value = record.get(replication_key)
            replication_key_value_parsed = parse(replication_key_value).strftime('%Y-%m-%dT%H:%M:%SZ')
            self.assertLessEqual(
                replication_key_value_parsed, bookmark_value_parsed,
                msg='Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.'
            )

            # Verify the data of the second sync is greater-equal to the bookmark from the first sync
            # We have added 'bookmark_date_for_2nd_sync' as the bookmark, it is more recent than
            #   the default start date and it will work as a simulated bookmark
            self.assertGreaterEqual(
                replication_key_value_parsed, parse(bookmark_date_for_2nd_sync).strftime('%Y-%m-%dT%H:%M:%SZ'),
                msg='Sync did not respect the bookmark, a record with a smaller replication-key value was synced.'
            )

        for record in records_sync_1:

            # Verify the first sync bookmark value is the max replication key value for a given stream
            replication_key_value = record.get(replication_key)
            replication_key_value_parsed = parse(replication_key_value).strftime('%Y-%m-%dT%H:%M:%SZ')
            self.assertLessEqual(
                replication_key_value_parsed, bookmark_value_parsed,
                msg='First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.'
            )
