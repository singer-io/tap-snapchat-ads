"""
Coverage tests for tap_snapchat_ads/__init__.py missing lines.
Covers: do_discover, maybe_parse_org_account_ids (invalid JSON), main() function.
"""
import io
import json
import sys
import unittest
from unittest import mock

import tap_snapchat_ads
from tap_snapchat_ads import do_discover, maybe_parse_org_account_ids


class TestDoDiscover(unittest.TestCase):
    def test_do_discover_writes_catalog_to_stdout(self):
        # lines 25-28: do_discover calls discover() and json.dumps to stdout
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            do_discover()
        output = mock_stdout.getvalue()
        catalog = json.loads(output)
        self.assertIn('streams', catalog)
        self.assertGreater(len(catalog['streams']), 0)


class TestMaybeParseOrgAccountIdsInvalidJson(unittest.TestCase):
    def test_invalid_json_string_raises_value_error(self):
        # lines 35-36: json.JSONDecodeError caught → ValueError raised
        config = {'org_account_ids': '[[[not_valid_json'}
        with self.assertRaises(ValueError) as ctx:
            maybe_parse_org_account_ids(config)
        self.assertIn('Error parsing org_account_ids string', str(ctx.exception))


class TestMainFunction(unittest.TestCase):
    """Tests for the main() function in __init__.py (lines 41-60)."""

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

    @mock.patch('tap_snapchat_ads.do_discover')
    @mock.patch('tap_snapchat_ads.SnapchatClient')
    @mock.patch('tap_snapchat_ads.singer.utils.parse_args')
    def test_main_discover_mode(self, mock_parse_args, mock_client_cls, mock_do_discover):
        """lines 41-57: main() in discover mode"""
        mock_parse_args.return_value = self._make_mock_args(discover_mode=True)
        mock_client_instance = mock.MagicMock()
        mock_client_cls.return_value.__enter__ = mock.MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

        tap_snapchat_ads.main()

        mock_do_discover.assert_called_once()

    @mock.patch('tap_snapchat_ads._sync')
    @mock.patch('tap_snapchat_ads.SnapchatClient')
    @mock.patch('tap_snapchat_ads.singer.utils.parse_args')
    def test_main_catalog_mode(self, mock_parse_args, mock_client_cls, mock_sync):
        """lines 58-60: main() in catalog/sync mode"""
        mock_args = self._make_mock_args(has_catalog=True, has_state=True)
        mock_parse_args.return_value = mock_args
        mock_client_instance = mock.MagicMock()
        mock_client_cls.return_value.__enter__ = mock.MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

        tap_snapchat_ads.main()

        mock_sync.assert_called_once()

    @mock.patch('tap_snapchat_ads.do_discover')
    @mock.patch('tap_snapchat_ads.SnapchatClient')
    @mock.patch('tap_snapchat_ads.singer.utils.parse_args')
    def test_main_no_state(self, mock_parse_args, mock_client_cls, mock_do_discover):
        """Covers: state = {} when parsed_args.state is None"""
        mock_parse_args.return_value = self._make_mock_args(discover_mode=True, has_state=False)
        mock_client_instance = mock.MagicMock()
        mock_client_cls.return_value.__enter__ = mock.MagicMock(return_value=mock_client_instance)
        mock_client_cls.return_value.__exit__ = mock.MagicMock(return_value=False)

        tap_snapchat_ads.main()
        mock_do_discover.assert_called_once()
