import io
import json
import unittest
from unittest import mock

import tap_snapchat_ads
from tap_snapchat_ads import maybe_parse_org_account_ids, do_discover


class TestMaybeParseOrgAccountIds(unittest.TestCase):
    config_with_list = {'org_account_ids':
                        [{'organisation_id': 'id1', 'ad_accounts': []},
                         {'organisation_id': 'id2', 'ad_accounts': ['ad_id1', 'ad_id2']}],
                        'start_date': '2024-02-24T00:00:00Z',}

    config_with_string = {'org_account_ids':
                          "[{\"organisation_id\":\"id1\",\"ad_accounts\":[]},{\"organisation_id\":\"id2\",\"ad_accounts\":[\"ad_id1\",\"ad_id2\"]}]",
                          'start_date': '2024-02-24T00:00:00Z',}

    config_with_bad_type = {'org_account_ids':
                            12345,
                            'start_date': '2024-02-24T00:00:00Z',}

    def test_with_list(self):
        """Test that config with org_account_ids of type list remains unchanged"""
        self.assertIsInstance(self.config_with_list["org_account_ids"], list)
        maybe_parse_org_account_ids(self.config_with_list)
        self.assertIsInstance(self.config_with_list["org_account_ids"], list)
        self.assertEqual(self.config_with_list["start_date"], '2024-02-24T00:00:00Z')

    def test_with_string(self):
        """Test that config with org_account_ids of type string is converted to type list"""
        self.assertIsInstance(self.config_with_string["org_account_ids"], str)
        maybe_parse_org_account_ids(self.config_with_string)
        self.assertIsInstance(self.config_with_string["org_account_ids"], list)
        self.assertEqual(self.config_with_string["start_date"], '2024-02-24T00:00:00Z')
        self.assertEqual(self.config_with_string, self.config_with_list)

    def test_with_bad_type(self):
        """Test that config with org_account_ids with unexpected type is unchanged by the function"""
        self.assertIsInstance(self.config_with_bad_type["org_account_ids"], int)
        maybe_parse_org_account_ids(self.config_with_bad_type)
        self.assertIsInstance(self.config_with_bad_type["org_account_ids"], int)
        self.assertEqual(self.config_with_bad_type["start_date"], '2024-02-24T00:00:00Z')

    def test_invalid_json_string_raises_value_error(self):
        """Test that a non-parseable JSON string raises ValueError"""
        config = {'org_account_ids': '[[[not_valid_json'}
        with self.assertRaises(ValueError) as ctx:
            maybe_parse_org_account_ids(config)
        self.assertIn('Error parsing org_account_ids string', str(ctx.exception))


# ---------------------------------------------------------------------------
# do_discover and main()
# ---------------------------------------------------------------------------

class TestDoDiscover(unittest.TestCase):
    def test_do_discover_writes_catalog_to_stdout(self):
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            do_discover()
        catalog = json.loads(mock_stdout.getvalue())
        self.assertIn('streams', catalog)
        self.assertGreater(len(catalog['streams']), 0)


class TestMainFunction(unittest.TestCase):
    def _make_mock_args(self, discover_mode=False, has_catalog=False, has_state=False):
        mock_args = mock.MagicMock()
        mock_args.config = {
            'client_id': 'cid',
            'client_secret': 'csec',
            'refresh_token': 'rtok',
            'start_date': '2021-01-01T00:00:00Z',
            'user_agent': 'test',
            'org_account_ids': [],
        }
        mock_args.state = {'bookmarks': {}} if has_state else None
        mock_args.discover = discover_mode
        mock_args.catalog = mock.MagicMock() if has_catalog else None
        return mock_args

    def _stub_client(self, mock_client_cls):
        mock_client_instance = mock.MagicMock()
        mock_client_cls.return_value.__enter__ = mock.MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

    @mock.patch('tap_snapchat_ads.do_discover')
    @mock.patch('tap_snapchat_ads.SnapchatClient')
    @mock.patch('tap_snapchat_ads.singer.utils.parse_args')
    def test_main_discover_mode(self, mock_parse_args, mock_client_cls, mock_do_discover):
        mock_parse_args.return_value = self._make_mock_args(discover_mode=True)
        self._stub_client(mock_client_cls)
        tap_snapchat_ads.main()
        mock_do_discover.assert_called_once()

    @mock.patch('tap_snapchat_ads._sync')
    @mock.patch('tap_snapchat_ads.SnapchatClient')
    @mock.patch('tap_snapchat_ads.singer.utils.parse_args')
    def test_main_catalog_mode(self, mock_parse_args, mock_client_cls, mock_sync):
        mock_parse_args.return_value = self._make_mock_args(has_catalog=True, has_state=True)
        self._stub_client(mock_client_cls)
        tap_snapchat_ads.main()
        mock_sync.assert_called_once()

    @mock.patch('tap_snapchat_ads.do_discover')
    @mock.patch('tap_snapchat_ads.SnapchatClient')
    @mock.patch('tap_snapchat_ads.singer.utils.parse_args')
    def test_main_no_state(self, mock_parse_args, mock_client_cls, mock_do_discover):
        mock_parse_args.return_value = self._make_mock_args(discover_mode=True, has_state=False)
        self._stub_client(mock_client_cls)
        tap_snapchat_ads.main()
        mock_do_discover.assert_called_once()
