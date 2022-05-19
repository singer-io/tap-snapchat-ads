from tap_snapchat_ads.sync import sync
import json
import unittest
from unittest import mock
from tap_snapchat_ads.client import SnapchatClient

class MockStream:
    def __init__(self, stream):
        self.stream = stream

# mock class for Catalog
class MockCatalog:
    def __init__(self, streams):
        self.streams = streams

    # mock class for get_stream
    def get_selected_streams(self, *args, **kwargs):
        streams = []
        for stream in self.streams:
            streams.append(MockStream(stream))
        return streams

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
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.write_schema")
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.process_records", side_effect=mocked_process_records)
@mock.patch("singer.metadata.to_map")
class TestBookmark(unittest.TestCase):
    """Class to test the bookmark write for different scenarios"""

    client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token", request_timeout=300)
    config = {"start_date": "2021-01-01T00:00:00Z"}

    def test_bookmark_no_parent(self, mocked_metadata, mocked_process_record, mocked_schema, mocked_client_get, mocked_access_token):
        """Test bookmark write when no parent is in the endpoint """
        state = {}

        sync(self.client, self.config, MockCatalog(['organizations']), state)
        expected_bookmark = '{"bookmarks": {"organizations": {"updated_at": "2022-04-16T05:44:39.787000Z"}}, "currently_syncing": null}'
        state = json.dumps(state)
        
        # Check whether bookmark is written as expected
        self.assertEqual(state, expected_bookmark, "Not getting expected bookmark value")


    def test_bookmark_with_parent(self, mocked_metadata, mocked_process_record, mocked_schema, mocked_client_get, mocked_access_token):
        """Test bookmark write when one parent and it's child is there in the endpoint"""
        state = {}

        sync(self.client, self.config, MockCatalog(['organizations', 'ad_accounts']), state)

        expected_bookmark = '{"bookmarks": {"ad_accounts": {"updated_at(parent_organization_id:organization_id)": "2022-04-16T05:44:39.787000Z"}, "organizations": {"updated_at": "2022-04-16T05:44:39.787000Z"}}, "currently_syncing": null}'
        state = json.dumps(state)

        # Check whether bookmark is written as expected
        self.assertEqual(state, expected_bookmark, "Not getting expected bookmark value")

    def test_bookmark_with_grand_parent(self, mocked_metadata, mocked_process_record, mocked_schema, mocked_client_get, mocked_access_token):
        """Test bookmark write when grandparent, it's child and child's child are there in the endpoint"""
        state = {}

        sync(self.client, self.config, MockCatalog(['organizations', 'ad_accounts', 'pixels']), state)

        expected_bookmark = '{"bookmarks": {"pixels": {"updated_at(parent_ad_account_id:adaccount_id)": "2022-04-16T05:44:39.787000Z"}, "ad_accounts": {"updated_at(parent_organization_id:organization_id)": "2022-04-16T05:44:39.787000Z"}, "organizations": {"updated_at": "2022-04-16T05:44:39.787000Z"}}, "currently_syncing": null}'
        state = json.dumps(state)

        # Check whether bookmark is written as expected
        self.assertEqual(state,expected_bookmark, "Not getting expected bookmark value")
