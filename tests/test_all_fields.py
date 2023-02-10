from tap_tester import runner, connections, menagerie
from base import SnapchatBase

class SnapchatAllFieldsTest(SnapchatBase):

    fields_to_remove = {
        # fields for which data is not generated
        'organizations': {
            'roles',
            'my_member_id',
            'my_display_name',
            'my_invited_email'
        },
        'ad_accounts': {
            'lifetime_spend_cap_micro'
        },
        'ad_squads': {
            'bid_micro',
            'cap_and_exclusion_config',
            'impression_goal',
            'lifetime_budget_micro',
            'placement_v2',
            'product_properties',
            'reach_and_frequency_micro',
            'reach_and_frequency_status',
            'reach_goal'
        },
        'ads': {
            'paying_advertiser_name',
            'third_party_swipe_tracking_urls'
        },
        'creatives': {
            'ad_to_lens_properties',
            'ad_to_message_properties',
            'app_install_properties',
            'collection_properties',
            'composite_properties',
            'deep_link_properties',
            'dynamic_render_properties',
            'longform_video_properties',
            'playback_type',
            'preview_creative_id',
            'preview_properties'
        },
        'funding_sources': {
            'available_credit_micro',
            'budget_spent_micro',
            'currency',
            'email',
            'end_date',
            'start_date',
            'total_budget_micro',
            'value_micro'
        },
        'media': {
            'lens_package_metadata'
        },
        # cannot publish pixels, thus not able to get 'effective_status' of the pixel
        'pixels': {
            'effective_status',
            'visible_to'
        },
        # These fields are not being replicated from the API, and it is confirmed by the support
        'roles': {
            'created_at',
            'updated_at'
        },
        # All the streams with 'targeting_*' contains a general schema structure
        # for all the stream. So, all streams will not contain all the fields.
        'targeting_advanced_demographics': {
            'country_code'
        },
        'targeting_age_groups': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_android_versions': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_carriers': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_connection_types': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_countries': {
            'city',
            'country_code',
            'metro',
            'parent_id',
            'path',
            'region',
            'source'
        },
        'targeting_device_makes': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_genders': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_interests_dlxc': {
            'country_code',
            'parent_id'
        },
        'targeting_interests_dlxp': {
            'country_code'
        },
        'targeting_interests_dlxs': {
            'country_code'
        },
        'targeting_interests_nln': {
            'country_code'
        },
        'targeting_interests_plc': {
            'country_code'
        },
        'targeting_interests_scls': {
            'country_code'
        },
        'targeting_ios_versions': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_languages': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_location_categories': {
            'country_code'
        },
        'targeting_metros': {
            'city',
            'parent_id',
            'path',
            'region',
            'source'
        },
        'targeting_os_types': {
            'country_code',
            'parent_id',
            'path',
            'source'
        },
        'targeting_postal_codes': {
            'parent_id',
            'path',
            'source'
        },
        'targeting_regions': {
            'city',
            'metro',
            'parent_id',
            'path',
            'source'
        }
    }

    def name(self):
        return "tap_tester_snapchat_ads_all_fields_test"

    def test_run(self):
        """
        Testing that all fields mentioned in the catalog are synced from the tap
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """
        expected_streams = self.expected_streams() - self.stats_streams - self.missing_targeting_streams

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1] for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = self.expected_primary_keys()[stream] | self.expected_replication_keys()[stream]

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)
