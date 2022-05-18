import unittest
from unittest import mock
from tap_snapchat_ads import SnapchatClient
import requests

class TestTimeoutValue(unittest.TestCase):
    """
    Test that "request_timeout" parameter works properly in various cases
    """
    def test_no_timeout_value_in_config(self):
        """
        Unit test to ensure that default "request_timeout" value is used when "requst_timeout"  is not passed in config
        """
        client = SnapchatClient("test", "test", "test", None, "test")

        # verify that the SnapchatClient has expected timeout value
        self.assertEqual(client.request_timeout, 300)

    def test_float_timeout_value_in_config(self):
        """
        Unit tests to ensure that "request_timeout" is set based on config float value
        """

        client = SnapchatClient("test", "test", "test", 100.10, "test")

        # verify that the SnapchatClient has expected timeout value
        self.assertEqual(client.request_timeout, 100.10)

    def test_string_timeout_value_in_config(self):
        """
        Unit tests to ensure that "request_timeout" is set based on config if string value is given in config
        """
        client = SnapchatClient("test", "test", "test", "100", "test")

        # verify that the SnapchatClient has expected timeout value
        self.assertEqual(client.request_timeout, 100)


    def test_empty_timeout_value_in_config(self):
        """
        Unit tests to ensure that "request_timeout" is set based on default value if empty value is given in config
        """
        client = SnapchatClient("test", "test", "test", "", "test")

        # verify that the SnapchatClient has expected timeout value
        self.assertEqual(client.request_timeout, 300)

    def test_string_zero_timeout_value_in_config(self):
        """
        Unit tests to ensure that "request_timeout" is set based on default value if string 0  is given in config
        """
        client = SnapchatClient("test", "test", "test", "0", "test")

        # verify that the SnapchatClient has expected timeout value
        self.assertEqual(client.request_timeout, 300)

    def test_wrong_string_timeout_value_in_config(self):
        """
        Unit tests to ensure that exception is raised when "request_timeout" is given in alphabet string
        """

        # Create SnapchatClient object
        with self.assertRaises(Exception) as e:
            client = SnapchatClient("test", "test", "test", "abc", "test")

        self.assertEqual(str(e.exception),"could not convert string to float: 'abc'","Enter timeout in numerics")

@mock.patch("time.sleep")        
@mock.patch('requests.Session.request')
class TestRequestTimeoutBackoff(unittest.TestCase):

    def test_request_timeout_backoff(self, mocked_request, mocked_sleep):
        """
            Verify request function is backing off 7 times on the Timeout exception for request function.
        """
        mocked_request.side_effect = requests.exceptions.Timeout
        client = SnapchatClient("test", "test", "test", 300, "test")

        with self.assertRaises(requests.exceptions.Timeout):
            client.request("get", "test", "test")

        # Verify that Session.send is called 7 times
        self.assertEqual(mocked_request.call_count, 7)
        
    def test_connection_error(self,  mocked_request, mocked_sleep):
        """
            Verify request function is backing off 7 times on the Connection exception for request function.
        """
        mocked_request.side_effect = requests.exceptions.ConnectionError
        client = SnapchatClient("test", "test", "test", 300, "test")
        
        with self.assertRaises(requests.exceptions.ConnectionError):
            client.request("get", "test", "test")
            
        # Verify that Session.send is called 5 times
        self.assertEqual(mocked_request.call_count, 7)
        
        
    def test_request_timeout_backoff_for_Snapchat_client(self,  mocked_request, mocked_sleep):
        """
            Verify request function is backing off 5 times on the Timeout exception for SnapchatClient creation.
        """

        mocked_request.side_effect = requests.exceptions.Timeout
        
        try:
            with SnapchatClient("test", "test", "test", 300, "test") as client:
                pass
        except requests.exceptions.Timeout:
            pass

        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
        
    def test_request_connection_error_for_Snapchat_client(self,  mocked_request, mocked_sleep):
        """
            Verify request function is backing off 5 times on the Connection exception for SnapchatClient creation.
        """
        mocked_request.side_effect = requests.exceptions.ConnectionError
        
        try:
            with SnapchatClient("test", "test", "test", 300, "test") as client:
                pass
        except requests.exceptions.ConnectionError:
            pass

        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
