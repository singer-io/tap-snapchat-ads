import logging
import unittest
from unittest import mock
import tap_snapchat_ads


class MockParseArgs:
    """Mock the parsed_args() in main"""

    config = {}
    state = {}
    catalog = {}
    discover = False

    def __init__(self, config, state, catalog, discover):
        self.config = config
        self.state = state
        self.catalog = catalog
        self.discover = discover


def get_args(config, state, catalog, discover):
    """Return the MockParseArgs object"""
    return MockParseArgs(config, state, catalog, discover)


@mock.patch("tap_snapchat_ads.SnapchatClient")
@mock.patch("singer.utils.parse_args")
class TestTimeoutValue(unittest.TestCase):
    """
    Test that "request_timeout" parameter works properly in various cases
    """

    mock_config = {
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "refresh_token": "YOUR_REFRESH_TOKEN",
        "user_agent": "tap-snapchat-ads api_user_email@example.com",
    }

    def test_no_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """
        Unit test to ensure that default "request_timeout" value is used when "requst_timeout"  is not passed in config
        """
        mock_config = {
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
            "refresh_token": "YOUR_REFRESH_TOKEN",
            "user_agent": "tap-snapchat-ads api_user_email@example.com",
        }
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_snapchat_ads.main()
        args, kwargs = mocked_client.call_args

        # verify that the request was called with expected timeout value
        self.assertEqual(args[3], 300)

    def test_float_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """
        Unit tests to ensure that "request_timeout" is set based on config float value
        """

        self.mock_config["request_timeout"] = 100.10

        # mock parse args
        mocked_parse_args.return_value = get_args(self.mock_config, {}, {}, False)

        # function call
        tap_snapchat_ads.main()
        args, kwargs = mocked_client.call_args

        # verify that the request was called with expected timeout value
        self.assertEqual(args[3], 100.10)

    def test_string_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """
        Unit tests to ensure that "request_timeout" is set based on config if string value is given in config
        """
        self.mock_config["request_timeout"] = "100"

        # mock parse args
        mocked_parse_args.return_value = get_args(self.mock_config, {}, {}, False)

        # function call
        tap_snapchat_ads.main()
        args, kwargs = mocked_client.call_args

        # verify that the request was called with expected timeout value
        self.assertEqual(args[3], 100)

    def test_empty_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """
        Unit tests to ensure that "request_timeout" is set based on default value if empty value is given in config
        """
        self.mock_config["request_timeout"] = ""

        # mock parse args
        mocked_parse_args.return_value = get_args(self.mock_config, {}, {}, False)

        # function call
        tap_snapchat_ads.main()
        args, kwargs = mocked_client.call_args

        # verify that the request was called with expected timeout value
        self.assertEqual(args[3], 300)

    def test_string_zero_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """
        Unit tests to ensure that "request_timeout" is set based on default value if string 0  is given in config
        """
        self.mock_config["request_timeout"] = "0"

        # mock parse args
        mocked_parse_args.return_value = get_args(self.mock_config, {}, {}, False)

        # function call
        tap_snapchat_ads.main()
        args, kwargs = mocked_client.call_args

        # verify that the request was called with expected timeout value
        self.assertEqual(args[3], 300)

    def test_wrong_string_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """
        Unit tests to ensure that exception is raised when "request_timeout" is given in alphabet string
        """
        self.mock_config["request_timeout"] = "abc"

        # mock parse args
        mocked_parse_args.return_value = get_args(self.mock_config, {}, {}, False)

        # function call
        with self.assertRaises(Exception) as e:
            tap_snapchat_ads.main()

        self.assertEqual(
            str(e.exception),
            "could not convert string to float: 'abc'",
            "Enter timeout in numerics",
        )
