from tap_tester import runner, connections
from base import SnapchatBase

class SnapchatParentChildIndependentTest(SnapchatBase):

    def name(self):
        return "tap_tester_snapchat_ads_parent_child_test"

    def test_first_level_child_streams(self):
        """
            Test case to verify that tap is working fine if only first level child streams are selected
        """
        # select first_level_child_streams only and run test
        first_level_child_streams = {"funding_sources", "billing_centers", "members", "roles", "ad_accounts", "product_catalogs"}
        self.run_test(first_level_child_streams)

    def test_second_level_child_streams(self):
        """
            Test case to verify that tap is working fine if only second level child streams are selected
        """
        # select second_level_child_streams only and run test
        second_level_child_streams = {"ad_account_stats_daily", "ad_account_stats_hourly", "audience_segments", "pixels", "media",
                               "creatives", "phone_numbers", "campaigns", "ad_squads", "ads", "product_sets"}
        # Don't have data for stats stream so excluding from test
        self.run_test(second_level_child_streams - self.stats_streams)

    def test_third_level_child_streams(self):
        """
            Test case to verify that tap is working fine if only third level child streams are selected
        """
        # select third_level_child_streams only and run test
        third_level_child_streams = {"pixel_domain_stats", "campaign_stats_daily", "campaign_stats_hourly", "ad_squad_stats_daily",
                                     "ad_squad_stats_hourly", "ad_stats_daily", "ad_squad_stats_hourly"}
        # Don't have data for stats streams so excluding from test(all streams are stats stream for third_level_child_streams)
        # self.run_test(third_level_child_streams - self.stats_streams)    

    def run_test(self, child_streams):
        """
            Testing that tap is working fine if only child streams are selected
            - Verify that if only child streams are selected then only child stream are replicated.
        """
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=child_streams)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(child_streams, synced_stream_names)
