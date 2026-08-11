"""
Coverage tests for tap_snapchat_ads/client.py missing lines.
Covers: get_access_token success, request success/rate-limit, get/post methods,
__enter__/__exit__, error_code with prefix, 5xx non-500/503, ValueError path.
"""
import unittest
from unittest import mock
from datetime import datetime, timedelta

import requests as req_lib

import tap_snapchat_ads.client as client_module
from tap_snapchat_ads.client import (
    SnapchatClient,
    raise_for_error,
    SnapchatError,
    Server5xxError,
)


def _make_200_response(json_body=None):
    """Build a mock requests.Response with status 200."""
    resp = mock.MagicMock()
    resp.status_code = 200
    resp.headers = {
        'X-Rate-Limit-Limit': '0',
        'X-Rate-Limit-Remaining': '100',
        'X-Rate-Limit-Reset': '0',
    }
    resp.json.return_value = json_body or {}
    resp.text = ''
    return resp


def _make_token_response(access_token='tok', expires_in=3600):
    return _make_200_response({'access_token': access_token, 'expires_in': expires_in})


# ---------------------------------------------------------------------------
# raise_for_error edge cases
# ---------------------------------------------------------------------------

class TestRaiseForErrorEdgeCases(unittest.TestCase):
    def _bad_resp(self, status_code, json_body=None, json_raises=False):
        resp = mock.MagicMock()
        resp.status_code = status_code
        resp.raise_for_status.side_effect = req_lib.HTTPError
        if json_raises:
            resp.json.side_effect = ValueError('bad json')
        else:
            resp.json.return_value = json_body or {}
        return resp

    def test_json_parse_error_gives_empty_response_json(self):
        # lines 137-138: except Exception → response_json = {}
        resp = self._bad_resp(400, json_raises=True)
        with self.assertRaises(client_module.SnapchatBadRequestError) as ctx:
            raise_for_error(resp)
        # Default message used since json failed
        self.assertIn('400', str(ctx.exception))

    def test_error_code_in_response_gets_comma_prefix(self):
        # line 142: error_code = ", " + error_code
        resp = self._bad_resp(400, json_body={'error_code': 'E1234', 'debug_message': 'bad req'})
        with self.assertRaises(client_module.SnapchatBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn('E1234', str(ctx.exception))

    def test_status_501_uses_server5xx_generic(self):
        # line 147: status_code > 500 and != 503 → Server5xxError
        resp = self._bad_resp(501)
        with self.assertRaises(Server5xxError):
            raise_for_error(resp)

    def test_status_504_uses_server5xx_generic(self):
        # line 147: another >500 non-503 code
        resp = self._bad_resp(504)
        with self.assertRaises(Server5xxError):
            raise_for_error(resp)

    def test_value_or_type_error_becomes_snapchat_error(self):
        # line 152: raise SnapchatError(err) from err when TypeError occurs
        class _WeirdResp:
            def raise_for_status(self):
                raise req_lib.HTTPError

            @property
            def status_code(self):
                raise TypeError('no status code')

            def json(self):
                return {}

        with self.assertRaises(SnapchatError):
            raise_for_error(_WeirdResp())


# ---------------------------------------------------------------------------
# get_access_token success path
# ---------------------------------------------------------------------------

class TestGetAccessTokenSuccess(unittest.TestCase):
    @mock.patch('requests.Session.request')
    def test_sets_access_token_and_expires(self, mock_request):
        # lines 192 (user-agent header), 212-216 (success body)
        mock_request.return_value = _make_token_response('mytoken', 1800)
        clx = SnapchatClient('cid', 'csec', 'rtok', 300, user_agent='test-agent')
        clx.get_access_token()
        # Access token should be set
        self.assertEqual(clx._SnapchatClient__access_token, 'mytoken')

    @mock.patch('requests.Session.request')
    def test_cached_token_not_refreshed(self, mock_request):
        # early return branch: token still valid
        mock_request.return_value = _make_token_response('tok1', 3600)
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        clx.get_access_token()   # sets token
        call_count_after_first = mock_request.call_count
        clx.get_access_token()   # should NOT call again (cached)
        self.assertEqual(mock_request.call_count, call_count_after_first)


# ---------------------------------------------------------------------------
# SnapchatClient.__enter__ / __exit__
# ---------------------------------------------------------------------------

class TestSnapchatClientContextManager(unittest.TestCase):
    @mock.patch('requests.Session.request')
    def test_enter_returns_self_and_exit_closes_session(self, mock_request):
        # lines 183, 186
        mock_request.return_value = _make_token_response()
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        result = clx.__enter__()
        self.assertIs(result, clx)
        clx.__exit__(None, None, None)   # should not raise


# ---------------------------------------------------------------------------
# request() success path, get(), post()
# ---------------------------------------------------------------------------

class TestRequestMethod(unittest.TestCase):
    @mock.patch('requests.Session.request')
    def test_successful_request_returns_json(self, mock_request):
        # lines 227-289: full request() body
        # First call → token exchange, second call → actual request
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'result': 'ok'}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.request('GET', path='organizations')
        self.assertEqual(data, {'result': 'ok'})

    @mock.patch('requests.Session.request')
    def test_get_method(self, mock_request):
        # line 292
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'items': []}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.get(url='https://test.com/v1/organizations')
        self.assertIsInstance(data, dict)

    @mock.patch('requests.Session.request')
    def test_post_method(self, mock_request):
        # line 295
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'created': True}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.post(url='https://test.com/v1/resource')
        self.assertIsInstance(data, dict)

    @mock.patch('requests.Session.request')
    def test_request_with_path_and_user_agent(self, mock_request):
        # lines cover: user_agent header in request, Content-Type for POST
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'ok': True}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300, user_agent='my-agent')
        data = clx.request('POST', path='some/resource')
        self.assertEqual(data.get('ok'), True)

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_rate_limit_warning_triggers_sleep(self, mock_request, mock_sleep):
        # rate_limit_percent_remaining < 5  → sleep
        rate_resp = _make_200_response({'rate': 'limited'})
        import time as time_module
        rate_resp.headers = {
            'X-Rate-Limit-Limit': '100',
            'X-Rate-Limit-Remaining': '4',   # 4% < 5%
            'X-Rate-Limit-Reset': str(int(time_module.time()) + 10),
        }
        mock_request.side_effect = [
            _make_token_response(),
            rate_resp,
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        clx.request('GET', path='organizations')
        mock_sleep.assert_called_once()

    @mock.patch('requests.Session.request')
    def test_invalid_json_response_raises(self, mock_request):
        """Covers the 'Catch invalid json response' block in request()"""
        token_resp = _make_token_response()
        bad_resp = mock.MagicMock()
        bad_resp.status_code = 200
        bad_resp.headers = {
            'X-Rate-Limit-Limit': '0',
            'X-Rate-Limit-Remaining': '100',
            'X-Rate-Limit-Reset': '0',
        }
        bad_resp.json.side_effect = ValueError('bad json')
        bad_resp.text = ''
        mock_request.side_effect = [token_resp, bad_resp]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        with self.assertRaises(ValueError):
            clx.request('GET', path='test')

    @mock.patch('requests.Session.request')
    def test_request_rebuilds_base_url_when_none(self, mock_request):
        # line 228: self.base_url = '{}/{}'.format(API_URL, API_VERSION)
        mock_request.side_effect = [_make_token_response(), _make_200_response({})]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        clx.base_url = None  # force the rebuild branch
        # Use path= (not url=) so the base_url rebuild is needed
        data = clx.request('GET', path='test_endpoint')
        self.assertIsInstance(data, dict)

    @mock.patch('requests.Session.request')
    def test_non_200_api_response_raises(self, mock_request):
        # lines 277-278: LOGGER.error + raise_for_error when status != 200 inside request()
        error_resp = mock.MagicMock()
        error_resp.status_code = 400
        error_resp.headers = {'X-Rate-Limit-Limit': '0',
                              'X-Rate-Limit-Remaining': '0',
                              'X-Rate-Limit-Reset': '0'}
        error_resp.text = 'bad request'
        error_resp.raise_for_status.side_effect = req_lib.HTTPError
        error_resp.json.return_value = {}
        mock_request.side_effect = [_make_token_response(), error_resp]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        with self.assertRaises(Exception):
            clx.request('GET', url='https://test.com/path')

    @mock.patch('requests.Session.request')
    def test_request_with_endpoint_kwarg(self, mock_request):
        """Covers endpoint extraction from kwargs"""
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'ep': 'test'}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.request('GET', url='https://test.com', endpoint='organizations')
        self.assertEqual(data.get('ep'), 'test')
