from tap_tester import runner, connections
from base import SnapchatBase

class SnapchatPaginationTest(SnapchatBase):

    page_size = 1

    def name(self):
        return "tap_tester_snapchat_ads_pagination_test"

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        # add page size of 1
        props['page_size'] = self.page_size
        return props

    def test_run(self):
        # we are getting duplicate records for 'id' fields for this stream
        # when asked support about this, but this is known behavior from the API side
        # Please refer card: https://jira.talendforge.org/browse/TDL-18686 for more details
        known_failing_streams = {"targeting_android_versions"}

        # phone_numbers: need to another phone number and verify it using OTP
        # pixels: not able to generate another record
        stream_to_skip = {"phone_numbers", "pixels"}

        # for this streams, page size is not supported for API calls
        # for organizations and ad_accounts, pagination is not being used in tap implementation, as they are part of profile selection
        pagination_not_supported_streams = {"funding_sources", "members",  "organizations", "ad_accounts"} | self.stats_streams

        streams_with_50_page_size = {"targeting_advanced_demographics", "targeting_interests_dlxc", "targeting_countries", "targeting_regions", "targeting_ios_versions", "targeting_carriers", "targeting_metros", "targeting_interests_scls", "targeting_interests_dlxs", "targeting_interests_dlxp", "targeting_interests_plc", "targeting_location_categories"}
        streams_with_1000_page_size = {"targeting_device_makes", "targeting_postal_codes", "targeting_interests_nln"}
        streams_with_1_page_size = {"product_sets", "targeting_connection_types", "creatives", "ad_squads", "targeting_os_types", "media", "ads", "product_catalogs", "billing_centers", "targeting_genders", "audience_segments", "targeting_languages", "targeting_age_groups", "roles", "campaigns"}

        # verify all the stream are either skipped or tested
        self.assertEqual(
            self.expected_streams() - known_failing_streams - stream_to_skip - pagination_not_supported_streams,
            streams_with_1_page_size | streams_with_50_page_size | streams_with_1000_page_size)

        self.page_size = 1
        self.run_test(streams_with_1_page_size)

        self.page_size = 50
        self.run_test(streams_with_50_page_size)

        self.page_size = 1000
        self.run_test(streams_with_1000_page_size)

    def run_test(self, streams):
        """
        Testing that the pagination works when there are records greater than the page size
        - Verify for each stream you can get multiple pages of data
        - Verify by pks that the data replicated matches the data we expect.
        """

        expected_streams = streams - self.missing_targeting_streams
        conn_id = connections.ensure_connection(self)

        # Select all streams and all fields within streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id, expected_streams)
        actual_fields_by_stream = runner.examine_target_output_for_fields()
        sync_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # verify that we can paginate with all fields selected
                minimum_record_count = self.page_size

                self.assertGreater(record_count_by_stream.get(stream, -1), minimum_record_count,
                    msg="The number of records is not over the stream max limit")

                expected_primary_key = self.expected_primary_keys().get(stream, set())
                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')
                expected_replication_key = self.expected_replication_keys().get(stream, set())
                expected_automatic_fields = expected_primary_key | expected_replication_key

                # verify that the automatic fields are sent to the target
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(expected_automatic_fields),
                    msg="The fields sent to the target don't include all automatic fields"
                )

                # verify we have more fields sent to the target than just automatic fields
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).symmetric_difference(expected_automatic_fields),
                    msg="The fields sent to the target don't include non-automatic fields"
                )

                # Verify we did not duplicate any records across pages
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_primary_key])
                                    for message in sync_messages]

                self.assertCountEqual(records_pks_list, set(records_pks_list), msg=f"We have duplicate records for {stream}")
