from tap_snapchat_ads.sync import sync
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

def mock_process_records(*args, **kwargs):
    """Mocking the process_records function"""
    # return date and record count
    return "2022-04-16T00:00:00Z", 1

def mock_get(*args, **kwargs):
    """Mocked get function to return the endpoint specific values"""
    stream_to_data_key_mapping = {
        "organizations": ["organizations", "organization"],
        "ad_accounts": ["adaccounts", "adaccount"],
        "pixels": ["pixels", "pixel"],
        "targeting_genders": ["targeting_genders", "gender"],
        "targeting_age_groups": ["targeting_age_groups", "age_group"],
        "targeting_languages": ["targeting_languages", "languages"],
    }

    endpoint = kwargs.get("endpoint")

    # sample response: {
    #     "request_status": "SUCCESS",
    #     "organizations": [{"sub_request_status": "SUCCESS","organization": {"id": "organization_id"}}]
    # }

    values = stream_to_data_key_mapping.get(endpoint)
    return {
        "request_status": "SUCCESS",
        values[0]: [{"sub_request_status": "SUCCESS", values[1]: {"id": endpoint + "_id"}}]
    }

@mock.patch("tap_snapchat_ads.client.SnapchatClient.get_access_token")
@mock.patch("tap_snapchat_ads.client.SnapchatClient.get", side_effect=mock_get)
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.process_records", side_effect=mock_process_records)
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.write_schema")
@mock.patch("singer.metadata.to_map")
@mock.patch("tap_snapchat_ads.sync.update_currently_syncing")
@mock.patch("tap_snapchat_ads.streams.update_currently_syncing")
@mock.patch("tap_snapchat_ads.streams.SnapchatAds.write_bookmark", return_value={})
class TestCurrentlySyncing(unittest.TestCase):
    """Class to test currently syncing streams"""

    # create SnapchatClient
    client = SnapchatClient(client_id="id", client_secret="secret", refresh_token="token", request_timeout=300)
    # config
    config = {"start_date": "2021-01-01T00:00:00Z"}

    def test_currently_syncing_with_parent(self, mocked_write_bookmark, mocked_currently_syncing_streams, mocked_currently_syncing_sync, mocked_metadata, mocked_schema, mocked_process_records, mock_get, mocked_get_access_token):
        """Test currently syncing stream when single parent and it"s child is present"""

        sync(self.client, self.config, MockCatalog(["ad_accounts"]), {})

        # create expected calls for which currently syncing is to be written
        expected_currently_syncing_calls_from_sync = [
            mock.call({}, 'organizations'),
            mock.call({}, None),
            mock.call({}, None)
        ]
        expected_currently_syncing_calls_from_streams = [
            mock.call({}, 'ad_accounts')
        ]
        # verify currently syncing is written for expected streams
        self.assertEqual(mocked_currently_syncing_sync.mock_calls, expected_currently_syncing_calls_from_sync)
        self.assertEqual(mocked_currently_syncing_streams.mock_calls, expected_currently_syncing_calls_from_streams)

    def test_currently_syncing_with_grand_parent(self, mocked_write_bookmark, mocked_currently_syncing_streams, mocked_currently_syncing_sync, mocked_metadata, mocked_schema, mocked_process_records, mock_get, mocked_get_access_token):
        """Test currently syncing stream when parent, it"s child and child"s child is present"""

        sync(self.client, self.config, MockCatalog(["pixels"]), {})

        # create expected calls for which currently syncing is to be written
        expected_currently_syncing_calls_from_sync = [
            mock.call({}, 'organizations'),
            mock.call({}, None),
            mock.call({}, None)
        ]
        expected_currently_syncing_calls_from_streams = [
            mock.call({}, 'ad_accounts'),
            mock.call({}, 'pixels')
        ]
        # verify currently syncing is written for expected streams
        self.assertEqual(mocked_currently_syncing_sync.mock_calls, expected_currently_syncing_calls_from_sync)
        self.assertEqual(mocked_currently_syncing_streams.mock_calls, expected_currently_syncing_calls_from_streams)

    def test_currently_syncing_with_interrupt(self, mocked_write_bookmark, mocked_currently_syncing_streams, mocked_currently_syncing_sync, mocked_metadata, mocked_schema, mocked_process_records, mock_get, mocked_get_access_token):
        """Test currently syncing stream with interrupt"""

        state = {"currently_syncing": "targeting_genders"}
        sync(self.client, self.config, MockCatalog(["targeting_age_groups", "targeting_genders", "targeting_languages"]), state)

        # create expected calls for which currently syncing is to be written
        expected_currently_syncing_calls_from_sync = [
            mock.call({"currently_syncing": "targeting_genders"}, 'targeting_genders'),
            mock.call({"currently_syncing": "targeting_genders"}, None),
            mock.call({"currently_syncing": "targeting_genders"}, 'targeting_languages'),
            mock.call({"currently_syncing": "targeting_genders"}, None),
            mock.call({"currently_syncing": "targeting_genders"}, None)
        ]
        # verify currently syncing is written for expected streams
        self.assertEqual(mocked_currently_syncing_sync.mock_calls, expected_currently_syncing_calls_from_sync)

    def test_currently_syncing_with_interrupt_old_stream_selected(self, mocked_write_bookmark, mocked_currently_syncing_streams, mocked_currently_syncing_sync, mocked_metadata, mocked_schema, mocked_process_records, mock_get, mocked_get_access_token):
        """Test currently syncing stream with interrupt, but the interrupted stream is not selected
           as a result, the state should set currently syncing as None at the end"""

        # set 'targeting_genders' as 'currently_syncing' in state file, but it is not selected
        state = {"currently_syncing": "targeting_genders"}
        sync(self.client, self.config, MockCatalog(["targeting_age_groups"]), state)

        # create expected calls for which currently syncing is to be written
        expected_currently_syncing_calls_from_sync = [
            mock.call({"currently_syncing": "targeting_genders"}, None)
        ]
        # verify currently syncing is set as None at the end
        self.assertEqual(mocked_currently_syncing_sync.mock_calls, expected_currently_syncing_calls_from_sync)
