from datetime import datetime as dt
from tap_tester import runner, connections, menagerie
from base import SnapchatBase

class SnapchatInterruptedSyncTest(SnapchatBase):

    def assertIsDateFormat(self, value, str_format):
        """
            Assertion Method that verifies a string value is a formatted datetime with
            the specified format.
        """
        try:
            _ = dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(f"Value: {value} does not conform to expected format: {str_format}") from err

    def name(self):
        return "tap_tester_snapchat_ads_interrupted_sync_test"

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.
        Expected State Structure:
            {
                "currently_syncing": "stream-name",
                "bookmarks": {
                    "stream-name-1": {
                        "replication-key(parent_parent_name_id:parent-id)": "bookmark-date",
                        "replication-key(parent_parent_name_id:parent-id)": "bookmark-date",
                        "replication-key(parent_parent_name_id:parent-id)": "bookmark-date"
                    },
                    "stream-name-2": {
                        "replication-key(parent_parent_name_id:parent-id)": "bookmark-date"
                    }
                }
            }
        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark are
            replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        self.START_DATE = "2020-01-01T00:00:00Z"
        conn_id = connections.ensure_connection(self, original_properties=False)

        expected_streams = {'ads', 'campaigns', 'ad_squads'}

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # de-select all the fields
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams, deselect_all_fields=True)

        # run sync
        record_count_by_stream_full_sync = self.run_and_verify_sync(conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        state = {
            "currently_syncing": "ad_squads",
            "bookmarks": {
                "campaigns": {
                    "updated_at(parent_ad_account_id:18d80268-379d-45f4-9869-45bb3316a69e)": "2022-04-27T12:14:58.680000Z",
                    "updated_at(parent_ad_account_id:cc8ba4e0-f36c-4f0e-9e2a-e8414caaaca1)": "2020-01-01T00:00:00Z"
                },
                "ad_squads": {
                    "updated_at(parent_ad_account_id:18d80268-379d-45f4-9869-45bb3316a69e)": "2022-04-15T08:52:44.359000Z"
                }
            }
        }
        menagerie.set_state(conn_id, state)

        # run sync
        record_count_by_stream_interrupted_sync = self.run_and_verify_sync(conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        # Checking resuming sync resulted in successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get('bookmarks'))

            # Verify final_state is equal to uninterrupted sync's state
            # (This is what the value would have been without an interruption and proves resuming succeeds)
            self.assertDictEqual(final_state, full_sync_state)

        # stream-level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # set expectations
                expected_replication_method = self.expected_replication_method()[stream]

                # gather results
                full_records = [message['data'] for message in synced_records_full_sync.get(stream, {}).get('messages', [])]
                full_record_count = record_count_by_stream_full_sync.get(stream, 0)
                interrupted_records = [message['data'] for message in synced_records_interrupted_sync.get(stream, {}).get('messages', [])]
                interrupted_record_count = record_count_by_stream_interrupted_sync.get(stream, 0)

                # gather expectations
                expected_primary_key = next(iter(self.expected_primary_keys()[stream]))
                expected_replication_key = next(iter(self.expected_replication_keys()[stream]))
                final_stream_bookmark = final_state['bookmarks'][stream]

                if expected_replication_method == self.INCREMENTAL:

                    # only verified interruptible syncing for account id: '18d80268-379d-45f4-9869-45bb3316a69e'
                    for account_id in ['18d80268-379d-45f4-9869-45bb3316a69e']:
                        final_bookmark = final_stream_bookmark.get(f'updated_at(parent_ad_account_id:{account_id})')
                        start_date_datetime = dt.strptime(self.START_DATE, "%Y-%m-%dT%H:%M:%SZ")

                        # Verify final bookmark saved match formatting standards for resuming sync
                        self.assertIsNotNone(final_bookmark)
                        self.assertIsInstance(final_bookmark, str)
                        self.assertIsDateFormat(final_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")

                        with self.subTest(customer_id=account_id):

                            # loop over interrupted state
                            if stream in state['bookmarks'].keys():
                                # if stream is set as currently syncing
                                if stream == state['currently_syncing']:
                                    interrupted_stream_bookmark = state['bookmarks'][stream]
                                    interrupted_bookmark = interrupted_stream_bookmark.get(f'updated_at(parent_ad_account_id:{account_id})')
                                    interrupted_bookmark_datetime = dt.strptime(interrupted_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")

                                    # - Verify resuming sync only replicates records with replication key values greater or equal to
                                    #       the state for streams that were replicated during the interrupted sync.
                                    # - Verify the interrupted sync replicates the expected record set
                                    #       All interrupted records are in full records
                                    for record in interrupted_records:
                                        with self.subTest(record_primary_key=record[expected_primary_key]):
                                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                                            self.assertGreaterEqual(rec_time, interrupted_bookmark_datetime)

                                        self.assertIn(record, full_records, msg='Incremental table record in interrupted sync not found in full sync')

                                    # Record count for all streams of interrupted sync match expectations
                                    full_records_after_interrupted_bookmark = 0
                                    for record in full_records:
                                        rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                                        if rec_time >= interrupted_bookmark_datetime:
                                            full_records_after_interrupted_bookmark += 1

                                    self.assertEqual(full_records_after_interrupted_bookmark, interrupted_record_count, \
                                                    msg='Expected {} records in each sync'.format(full_records_after_interrupted_bookmark))

                                # if stream has synced record successfully
                                else:
                                    # verify we collected 0 records for stream that are already synced
                                    self.assertEqual(interrupted_record_count, 0)

                            else:
                                # - Verify resuming sync only replicates records with replication key values greater or equal to
                                #       the state for streams that were replicated during the interrupted sync.
                                # - Verify resuming sync replicates all records that were found in the full sync (un-interupted)
                                for record in interrupted_records:
                                    with self.subTest(record_primary_key=record[expected_primary_key]):
                                        rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                                        self.assertGreaterEqual(rec_time, start_date_datetime)

                                    with self.subTest(record_primary_key=record[expected_primary_key]):
                                        self.assertIn(record, full_records, msg='Unexpected record replicated in resuming sync.')

                                for record in full_records:
                                    with self.subTest(record_primary_key=record[expected_primary_key]):
                                        self.assertIn(record, interrupted_records, msg='Record missing from resuming sync.' )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams do not save bookmarked values at the conclusion of a successful sync
                    self.assertNotIn(stream, full_sync_state['bookmarks'].keys())
                    self.assertNotIn(stream, final_state['bookmarks'].keys())

                    # Verify first and second sync have the same records
                    self.assertEqual(full_record_count, interrupted_record_count)
                    for rec in interrupted_records:
                        self.assertIn(rec, full_records, msg='full table record in interrupted sync not found in full sync')
