from singer import utils
import json
import humps
import unittest
from unittest import mock
import tap_snapchat_ads
from tap_snapchat_ads.client import LOGGER, SnapchatClient
from tap_snapchat_ads.sync import sync_endpoint

def mocked_process_records(catalog, stream_name, records, time_extracted, bookmark_field, max_bookmark_value, last_datetime):
    """Mocking the process_records function"""
    count = 1
    mocked_records = {"updated_at": "2022-04-16T05:44:39.787000Z", "name": "Stitch"}
    max_bookmark = mocked_records.get("updated_at")

    return max_bookmark, count

def mocked_get(*args, **kwargs):
    """Mocked get function to return the endpoint specific values"""

    endpoint = kwargs.get('endpoint')

    if endpoint == 'organizations':
        return {
            "request_status": "SUCCESS",
            "organizations": [{"sub_request_status": "SUCCESS","organization": {"id": "organization_id"}}]
        }
    if endpoint == 'ad_accounts':
        return {
            "request_status": "SUCCESS",
            "adaccounts": [{"sub_request_status": "SUCCESS","adaccount": {"id": "adaccount_id"}}]
        }
    if endpoint == 'pixels':
        return {
            "request_status": "SUCCESS",
            "pixels": [{"sub_request_status": "SUCCESS","pixel": {"id": "pixel_id"}}]
        }

@mock.patch("tap_snapchat_ads.client.SnapchatClient.get_access_token")
@mock.patch("tap_snapchat_ads.client.SnapchatClient.get",side_effect=mocked_get)
@mock.patch("tap_snapchat_ads.sync.write_schema")
@mock.patch("tap_snapchat_ads.sync.process_records", side_effect=mocked_process_records)
@mock.patch("singer.metadata.to_map")
class TestBookmark(unittest.TestCase):
    """Class to test the bookmark write for different scenarios"""

    def test_bookmark_no_parent(self, mocked_metadata, mocked_process_record, mocked_schema, mocked_client_get, mocked_access_token):
        """Test bookmark write when no parent is in the endpoint """

        client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token")
        dummy_config = {"start_date": "2021-01-01T00:00:00Z"}
        stream_name = "organizations"
        dummy_catalog = {}
        dummy_state = {}
        sync_streams = ["organizations"]
        selected_streams = ["organizations"]
        endpoint_config = {
            "data_key_record": "organization",
            "replication_keys": ["updated_at"],
            "data_key_array": "organizations",
            "key_properties": ["id"],
        }

        sync_endpoint(client, dummy_config, dummy_catalog, dummy_state, stream_name, endpoint_config, sync_streams, selected_streams)
        expected_bookmark = '{"bookmarks": {"organizations": {"updated_at": "2022-04-16T05:44:39.787000Z"}}}'
        state = json.dumps(dummy_state)
        
        # Check whether bookmark is written as expected
        self.assertEqual(state, expected_bookmark, "Not getting expected bookmark value")


    def test_bookmark_with_parent(self, mocked_metadata, mocked_process_record, mocked_schema, mocked_client_get, mocked_access_token):
        """Test bookmark write when one parent and it's child is there in the endpoint"""

        client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token")
        dummy_config = {"start_date": "2021-01-01T00:00:00Z"}
        stream_name = "organizations"
        dummy_catalog = {}
        dummy_state = {}
        sync_streams = ["organizations", "ad_accounts"]
        selected_streams = ["organizations", "ad_accounts"]
        endpoint_config = {
            "data_key_record": "organization",
            "replication_keys": ["updated_at"],
            "data_key_array": "organizations",
            "key_properties": ["id"],
            'children':{
                'ad_accounts':{
                    "data_key_record": "adaccount",
                    "replication_keys": ["updated_at"],
                    "data_key_array": "adaccounts",
                    "key_properties": ["id"],
                    'parent':'organizations'
                }
            }
        }

        sync_endpoint(client, dummy_config, dummy_catalog, dummy_state, stream_name, endpoint_config, sync_streams, selected_streams)

        expected_bookmark = '{"bookmarks": {"ad_accounts": {"updated_at(parent_organizations_id:organization_id)": "2022-04-16T05:44:39.787000Z"}, "organizations": {"updated_at": "2022-04-16T05:44:39.787000Z"}}}'
        state = json.dumps(dummy_state)
        
        # Check whether bookmark is written as expected
        self.assertEqual(state, expected_bookmark, "Not getting expected bookmark value")

    def test_bookmark_with_grand_parent(self, mocked_metadata, mocked_process_record, mocked_schema, mocked_client_get, mocked_access_token):
        """Test bookmark write when grandparent, it's child and child's child are there in the endpoint"""

        client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token")
        dummy_config = {"start_date": "2021-01-01T00:00:00Z"}
        stream_name = "organizations"
        dummy_catalog = {}
        dummy_state = {}
        sync_streams = ["organizations", "ad_accounts", "pixels"]
        selected_streams = ["organizations", "ad_accounts", "pixels"]
        endpoint_config = {
            "data_key_record": "organization",
            "replication_keys": ["updated_at"],
            "data_key_array": "organizations",
            "key_properties": ["id"],
            'children':{
                'ad_accounts':{
                    "data_key_record": "adaccount",
                    "replication_keys": ["updated_at"],
                    "data_key_array": "adaccounts",
                    "key_properties": ["id"],
                    'parent':'organizations',
                    'children':{
                        'pixels':{
                            "data_key_record": "adaccount",
                            "replication_keys": ["updated_at"],
                            "data_key_array": "adaccounts",
                            "key_properties": ["id"],
                            'parent':'ad_accounts'
                        }
                    }
                    
                }
            }
        }

        sync_endpoint(client, dummy_config, dummy_catalog, dummy_state, stream_name, endpoint_config, sync_streams, selected_streams)

        expected_bookmark = '{"bookmarks": {"pixels": {"updated_at(parent_ad_accounts_id:adaccount_id)": "2021-01-01T00:00:00Z"}, "ad_accounts": {"updated_at(parent_organizations_id:organization_id)": "2022-04-16T05:44:39.787000Z"}, "organizations": {"updated_at": "2022-04-16T05:44:39.787000Z"}}}'
        state = json.dumps(dummy_state)

        # Check whether bookmark is written as expected
        self.assertEqual(state,expected_bookmark, "Not getting expected bookmark value")
