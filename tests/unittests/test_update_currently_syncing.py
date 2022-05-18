import unittest
from unittest import mock
from tap_snapchat_ads.client import SnapchatClient
from tap_snapchat_ads.streams import SnapchatAds, Organizations

def mock_process_records(*args, **kwargs):
    """Mocking the process_records function"""
    # return date and record count
    return "2022-04-16T00:00:00Z", 1

def mock_get(*args, **kwargs):
    """Mocked get function to return the endpoint specific values"""

    endpoint = kwargs.get("endpoint")

    if endpoint == "organizations":
        return {
            "request_status": "SUCCESS",
            "organizations": [{"sub_request_status": "SUCCESS","organization": {"id": "organization_id"}}]
        }
    if endpoint == "ad_accounts":
        return {
            "request_status": "SUCCESS",
            "adaccounts": [{"sub_request_status": "SUCCESS","adaccount": {"id": "ad_account_id"}}]
        }
    if endpoint == "pixels":
        return {
            "request_status": "SUCCESS",
            "pixels": [{"sub_request_status": "SUCCESS","pixel": {"id": "pixel_id"}}]
        }

@mock.patch("tap_snapchat_ads.client.SnapchatClient.get_access_token")
@mock.patch("tap_snapchat_ads.client.SnapchatClient.get", side_effect=mock_get)
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.process_records", side_effect=mock_process_records)
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.write_schema")
@mock.patch("singer.metadata.to_map")
@mock.patch("tap_snapchat_ads.streams.update_currently_syncing")
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.write_bookmark", return_value={})
class TestCurrentlySyncing(unittest.TestCase):
    """Class to test currently syncing streams"""

    def test_currently_syncing_with_parent(self, mocked_write_bookmark, mocked_currently_syncing, mocked_metadata, mocked_schema, mocked_process_records, mock_get, mocked_get_access_token):
        """Test currently syncing stream when single parent and it"s child is present"""

        # create SnapchatClient
        client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token", request_timeout=300)
        # config
        config = {"start_date": "2021-01-01T00:00:00Z"}
        # all streams streams other than "targeting" streams, syncing will start from "organizations"
        stream_name = "organizations"
        # selected stream is "ad_accounts"
        selected_streams = ["ad_accounts"]
        # List of all the streams need to be synced
        sync_streams = ["organizations","ad_accounts"]
        # dummy organization object
        organization = Organizations

        # create SnapchatAds stream object and call sync_endpoint
        stream_obj = SnapchatAds()
        stream_obj.sync_endpoint(client, config, {}, {}, stream_name, organization, sync_streams, selected_streams)

        # create expected calls for which currently syncing is to be written
        expected_currently_syncing_calls = [
            mock.call({}, 'ad_accounts')
        ]
        # verify currently syncing is written for expected streams
        self.assertEqual(mocked_currently_syncing.mock_calls, expected_currently_syncing_calls)

    def test_currently_syncing_with_grand_parent(self, mocked_write_bookmark, mocked_currently_syncing, mocked_metadata, mocked_schema, mocked_process_records, mock_get, mocked_get_access_token):
        """Test currently syncing stream when parent, it"s child and child"s child is present"""

        # create SnapchatClient
        client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token", request_timeout=300)
        # config
        config = {"start_date": "2021-01-01T00:00:00Z"}
        # all streams streams other than "targeting" streams, syncing will start from "organizations"
        stream_name = "organizations"
        # selected stream is "pixels"
        selected_streams = ["pixels"]
        # List of all the streams need to be synced
        sync_streams = ["organizations", "ad_accounts", "pixels"]
        # dummy organization object
        organization = Organizations

        # create SnapchatAds stream object and call sync_endpoint
        stream_obj = SnapchatAds()
        stream_obj.sync_endpoint(client, config, {}, {}, stream_name, organization, sync_streams, selected_streams)

        # create expected calls for which currently syncing is to be written
        expected_currently_syncing_calls = [
            mock.call({}, 'ad_accounts'),
            mock.call({}, 'pixels')
        ]
        # verify currently syncing is written for expected streams
        self.assertEqual(mocked_currently_syncing.mock_calls, expected_currently_syncing_calls)
