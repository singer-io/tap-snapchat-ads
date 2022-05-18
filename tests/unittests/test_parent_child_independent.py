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
    def __init__(self,stream):
        self.stream = stream
    
    def get_stream(self, stream):
        """Returns Mocked stream"""
        return MockStream

class TestSnapchatAdsStreamsindependent(unittest.TestCase):
    
    @mock.patch('tap_snapchat_ads.streams.singer.write_schema')
    def test_write_schema(self, mock_write_schema):
        """Test for validating only schemas of selected streams are written"""
        
        sync_streams= ['pixel_domain_stats', 'pixels', 'ad_accounts', 'organizations']
        selected_streams = ['pixel_domain_stats']
        stream_name = "pixel_domain_stats"
        catalog = MockCatalog('pixel_domain_stats')

        obj = SnapchatAds()
        obj.write_schema(catalog,stream_name,sync_streams,selected_streams)
        
        #Asserts only schema for selected streams are written
        mock_write_schema.assert_called_with('pixel_domain_stats', {} , 'test')
