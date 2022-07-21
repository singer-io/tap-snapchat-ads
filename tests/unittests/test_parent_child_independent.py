import unittest
from unittest import mock
from singer.schema import Schema
from tap_snapchat_ads.streams import SnapchatAds


class MockStream():
    """Mocked Stream"""
    schema = Schema()
    key_properties = "test"


class MockCatalog():
    '''Mocked the Catalog class.'''

    def __init__(self, stream):
        self.stream = stream

    def get_stream(self, stream):
        """Returns Mocked stream"""
        return MockStream


class TestSnapchatAdsStreamsindependent(unittest.TestCase):

    @mock.patch('tap_snapchat_ads.streams.singer.write_schema')
    def test_write_schema(self, mock_write_schema):
        """Test for validating only schemas of selected streams are written"""

        sync_streams = ['pixel_domain_stats', 'pixels', 'ad_accounts', 'organizations']
        selected_streams = ['pixel_domain_stats']
        stream_name = "pixel_domain_stats"
        catalog = MockCatalog('pixel_domain_stats')

        obj = SnapchatAds()
        obj.write_schema(catalog, stream_name, sync_streams, selected_streams)

        # Asserts only schema for selected streams are written
        mock_write_schema.assert_called_with('pixel_domain_stats', {}, 'test')


class TestExtractOrgAdAccountData(unittest.TestCase):
    stream_obj = SnapchatAds()

    def test_extract_selected_profile_data_orgs(self):
        """
        Test for validating the response when no organization ids are sent as argument
        """
        config = {'org_account_ids': [{"organisation_id": "", "ad_accounts": []}]}
        client = {}
        stream_name = 'organization'

        self.assertEqual({'organization': []}, self.stream_obj.extract_selected_profile_data(config,
                                                                                             client, stream_name))

    def test_extract_selected_profile_data_ad_acc(self):
        """
        Test for validating the response when no ad_account ids are sent as argument
        """
        config = {'org_account_ids': [{"organisation_id": "abc", "ad_accounts": []}]}
        client = {}
        stream_name = 'ad_accounts'
        self.assertEqual({'ad_accounts': []}, self.stream_obj.extract_selected_profile_data(config, client,
                                                                                            stream_name, 'abc'))
