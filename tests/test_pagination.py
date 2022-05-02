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
        pagination_not_supported_streams = {"funding_sources", "members", "ad_account_stats_daily", "ad_account_stats_hourly", "pixel_domain_stats", "campaign_stats_daily", "campaign_stats_hourly", "ad_squad_stats_daily", "ad_squad_stats_hourly", "ad_stats_daily",  "ad_stats_hourly"}

        paging_supported_streams = self.expected_streams() - pagination_not_supported_streams - stream_to_skip - known_failing_streams

        self.page_size = 1
        self.run_test(paging_supported_streams - {"targeting_device_makes", "targeting_postal_codes", "targeting_interests_nln", "targeting_advanced_demographics", "targeting_interests_dlxc", "targeting_countries", "targeting_regions", "targeting_ios_versions", "targeting_carriers", "targeting_metros", "targeting_interests_scls", "targeting_interests_dlxs", "targeting_interests_dlxp", "targeting_interests_plc", "targeting_location_categories"})

        self.page_size = 50
        self.run_test({"targeting_advanced_demographics", "targeting_interests_dlxc", "targeting_countries", "targeting_regions", "targeting_ios_versions", "targeting_carriers", "targeting_metros", "targeting_interests_scls", "targeting_interests_dlxs", "targeting_interests_dlxp", "targeting_interests_plc", "targeting_location_categories"})

        self.page_size = 1000
        self.run_test({"targeting_device_makes", "targeting_postal_codes", "targeting_interests_nln"})

    def run_test(self, streams):
        """
        Testing that the pagination works when there are records greater than the page size
        - Verify for each stream you can get multiple pages of data
        - Verify by pks that the data replicated matches the data we expect.
        """

        expected_streams = streams
        conn_id = connections.ensure_connection(self)

        # Select all streams and all fields within streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id, expected_streams)
        actual_fields_by_stream = runner.examine_target_output_for_fields()
        sync_records = runner.get_records_from_target_output()

        for stream in expected_streams - {"organizations"}:
            with self.subTest(stream=stream):

                # verify that we can paginate with all fields selected
                minimum_record_count = self.page_size

                self.assertGreater(record_count_by_stream.get(stream, -1), minimum_record_count,
                    msg="The number of records is not over the stream max limit")

                expected_pk = self.expected_primary_keys()
                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                # verify that the automatic fields are sent to the target
                if stream != "roles":
                    self.assertTrue(
                        actual_fields_by_stream.get(stream, set()).issuperset(
                            expected_pk.get(stream, set()) |
                            self.expected_replication_keys().get(stream, set())),
                        msg="The fields sent to the target don't include all automatic fields"
                    )

                # verify we have more fields sent to the target than just automatic fields
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).symmetric_difference(
                        expected_pk.get(stream, set()) |
                        self.expected_replication_keys().get(stream, set())),
                    msg="The fields sent to the target don't include non-automatic fields"
                )

                # Verify we did not duplicate any records across pages
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_pk.get(stream, set())])
                                    for message in sync_messages]

                self.assertCountEqual(records_pks_list, set(records_pks_list), msg=f"We have duplicate records for {stream}")
