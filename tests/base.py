import time
import unittest
import os
from datetime import datetime as dt
from datetime import timedelta
from tap_tester import connections, menagerie, runner


class SnapchatBase(unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """

    START_DATE = ""
    DATETIME_FMT = {
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    }
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"

    # TDL-21946 Work-Item to generate test data for following list of streams
    stats_streams = {"ad_account_stats_daily", "ad_account_stats_hourly", "pixel_domain_stats",
                     "campaign_stats_daily", "campaign_stats_hourly", "ad_squad_stats_daily",
                     "ad_squad_stats_hourly", "ad_stats_daily",  "ad_stats_hourly"}
    # Currently there is no data available for following targeting streams
    missing_targeting_streams = {"targeting_interests_dlxp", "targeting_interests_plc"}

    # Currently there is no data available for following targeting streams
    targeting_streams = {"targeting_interests_dlxp", "targeting_interests_plc"}

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-snapchat-ads"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.snapchat-ads"

    def setUp(self):
        required_env = {
            "TAP_SNAPCHAT_ADS_CLIENT_ID",
            "TAP_SNAPCHAT_ADS_CLIENT_SECRET",
            "TAP_SNAPCHAT_ADS_REFRESH_TOKEN",
            "TAP_SNAPCHAT_ADS_ORGANIZATION_ID",
            "TAP_SNAPCHAT_ADS_AD_ACCOUNT_IDS"
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            "swipe_up_attribution_window": "28_DAY",
            "view_attribution_window": "7_DAY",
            "omit_empty": "true",
            "targeting_country_codes": "us, ca",
            "start_date": "2020-01-01T00:00:00Z",
            "org_account_ids": [{"organisation_id": os.getenv("TAP_SNAPCHAT_ADS_ORGANIZATION_ID"), "ad_accounts":
                                 os.getenv("TAP_SNAPCHAT_ADS_AD_ACCOUNT_IDS", "").split(",")}]
        }

        if original:
            return return_value

        return_value["start_date"] = self.START_DATE
        return return_value

    def get_credentials(self):
        """Return creds from env variables"""
        return {
            "client_id": os.getenv("TAP_SNAPCHAT_ADS_CLIENT_ID"),
            "client_secret": os.getenv("TAP_SNAPCHAT_ADS_CLIENT_SECRET"),
            "refresh_token": os.getenv("TAP_SNAPCHAT_ADS_REFRESH_TOKEN")
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        incremental_metadata = {
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.REPLICATION_KEYS: {"updated_at"},
            self.OBEYS_START_DATE: True
        }
        stats_metadata = {
            self.PRIMARY_KEYS: {"id", "start_time"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.REPLICATION_KEYS: {"end_time"},
            self.OBEYS_START_DATE: True
        }
        full_table_metadata = {
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.FULL_TABLE,
            self.OBEYS_START_DATE: False
        }
        return {
            "organizations": incremental_metadata,
            "funding_sources": incremental_metadata,
            "billing_centers": incremental_metadata,
            "members": incremental_metadata,
            "roles": full_table_metadata,
            "ad_accounts": incremental_metadata,
            "ad_account_stats_daily": stats_metadata,
            "ad_account_stats_hourly": stats_metadata,
            "audience_segments": incremental_metadata,
            "pixels": incremental_metadata,
            "pixel_domain_stats": full_table_metadata,
            "media": incremental_metadata,
            "creatives": incremental_metadata,
            "phone_numbers": incremental_metadata,
            "campaigns": incremental_metadata,
            "campaign_stats_daily": stats_metadata,
            "campaign_stats_hourly": stats_metadata,
            "ad_squads": incremental_metadata,
            "ad_squad_stats_daily": stats_metadata,
            "ad_squad_stats_hourly": stats_metadata,
            "ads": incremental_metadata,
            "ad_stats_daily": stats_metadata,
            "ad_stats_hourly": stats_metadata,
            "product_catalogs": incremental_metadata,
            "product_sets": full_table_metadata,
            "targeting_age_groups": full_table_metadata,
            "targeting_genders": full_table_metadata,
            "targeting_languages": full_table_metadata,
            "targeting_advanced_demographics": full_table_metadata,
            "targeting_connection_types": full_table_metadata,
            "targeting_os_types": full_table_metadata,
            "targeting_ios_versions": full_table_metadata,
            "targeting_android_versions": full_table_metadata,
            "targeting_carriers": full_table_metadata,
            "targeting_device_makes": full_table_metadata,
            "targeting_countries": full_table_metadata,
            "targeting_regions": full_table_metadata,
            "targeting_metros": full_table_metadata,
            "targeting_postal_codes": full_table_metadata,
            "targeting_interests_scls": full_table_metadata,
            "targeting_interests_dlxs": full_table_metadata,
            "targeting_interests_dlxc": full_table_metadata,
            "targeting_interests_dlxp": full_table_metadata,
            "targeting_interests_nln": full_table_metadata,
            "targeting_interests_plc": full_table_metadata,
            "targeting_location_categories": full_table_metadata,
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    #########################
    #   Helper Methods      #
    #########################

    def select_found_catalogs(self, conn_id, catalogs, only_streams=None, deselect_all_fields: bool = False, non_selected_props=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if only_streams and catalog["stream_name"] not in only_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = non_selected_props if not deselect_all_fields else []
            if deselect_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {})
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               catalog,
                                                               schema,
                                                               additional_md=additional_md,
                                                               non_selected_fields=non_selected_properties)

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        print(found_catalog_names)
        self.assertSetEqual(set(self.expected_metadata().keys()), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, streams=None):
        """Run sync, verify we replicated some records and return record count by streams"""
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              streams if streams else self.expected_streams(),
                                                              self.expected_primary_keys())

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def dt_to_ts(self, dtime):
        """Convert date to epoch time"""
        for date_format in self.DATETIME_FMT:
            try:
                date_stripped = int(time.mktime(dt.strptime(dtime, date_format).timetuple()))
                return date_stripped
            except ValueError:
                continue

    def is_incremental(self, stream):
        """Boolean function to check is the stream is INCREMENTAL of not"""
        return self.expected_metadata()[stream][self.REPLICATION_METHOD] == self.INCREMENTAL
