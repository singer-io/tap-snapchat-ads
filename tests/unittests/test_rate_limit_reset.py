import unittest
from unittest import mock

from tap_snapchat_ads import SnapchatClient
from tap_snapchat_ads.client import MAX_RATE_LIMIT_WAIT, Server429Error, normalize_unix_timestamp

NOW = 1_700_000_000.0


def _response(status=200, headers=None, body=None):
    resp = mock.Mock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.json.return_value = body if body is not None else {'ok': True}
    resp.text = ''
    if status != 200:
        import requests
        resp.raise_for_status.side_effect = requests.HTTPError
    return resp


def _token():
    return _response(200, body={'access_token': 'tok', 'expires_in': 3600})


def _limited(reset):
    return _response(200, {
        'X-Rate-Limit-Limit': '100',
        'X-Rate-Limit-Remaining': '1',
        'X-Rate-Limit-Reset': str(reset),
    })


@mock.patch('tap_snapchat_ads.client.time.time', return_value=NOW)
@mock.patch('tap_snapchat_ads.client.time.sleep')
@mock.patch('requests.Session.request')
class TestRateLimitReset(unittest.TestCase):
    def _call(self, mock_request, resp):
        mock_request.side_effect = [_token(), resp]
        return SnapchatClient('cid', 'csec', 'rtok', 300).request('GET', path='organizations')

    def test_normalize_seconds_and_milliseconds(self, *_):
        self.assertEqual(normalize_unix_timestamp(int(NOW)), int(NOW))
        self.assertEqual(normalize_unix_timestamp(int(NOW) * 1000), int(NOW))

    def test_seconds_reset_sleeps_until_reset(self, mock_request, mock_sleep, _time):
        self._call(mock_request, _limited(int(NOW) + 30))
        mock_sleep.assert_called_once_with(30)

    def test_milliseconds_reset_sleeps_until_reset(self, mock_request, mock_sleep, _time):
        self._call(mock_request, _limited((int(NOW) + 30) * 1000))
        mock_sleep.assert_called_once_with(30)

    def test_expired_reset_does_not_sleep(self, mock_request, mock_sleep, _time):
        self._call(mock_request, _limited(int(NOW) - 100))
        mock_sleep.assert_not_called()

    def test_missing_reset_does_not_sleep(self, mock_request, mock_sleep, _time):
        resp = _response(200, {'X-Rate-Limit-Limit': '100', 'X-Rate-Limit-Remaining': '1'})
        self._call(mock_request, resp)
        mock_sleep.assert_not_called()

    def test_wait_capped_at_24_hours(self, mock_request, mock_sleep, _time):
        self._call(mock_request, _limited(int(NOW) + 3 * MAX_RATE_LIMIT_WAIT))
        mock_sleep.assert_called_once_with(MAX_RATE_LIMIT_WAIT)

    def test_wait_exactly_24_hours_not_capped_down(self, mock_request, mock_sleep, _time):
        self._call(mock_request, _limited(int(NOW) + MAX_RATE_LIMIT_WAIT))
        mock_sleep.assert_called_once_with(MAX_RATE_LIMIT_WAIT)

    def test_above_threshold_does_not_sleep(self, mock_request, mock_sleep, _time):
        resp = _response(200, {'X-Rate-Limit-Limit': '100', 'X-Rate-Limit-Remaining': '50',
                               'X-Rate-Limit-Reset': str(int(NOW) + 30)})
        self._call(mock_request, resp)
        mock_sleep.assert_not_called()

    def test_429_is_retried_then_succeeds(self, mock_request, mock_sleep, _time):
        mock_request.side_effect = [_token(), _response(429), _response(200, body={'ok': 1})]
        data = SnapchatClient('cid', 'csec', 'rtok', 300).request('GET', path='organizations')
        self.assertEqual(data, {'ok': 1})
        self.assertEqual(mock_request.call_count, 3)
        mock_sleep.assert_called()

    def test_429_retries_exhausted_raises(self, mock_request, mock_sleep, _time):
        mock_request.side_effect = [_token()] + [_response(429)] * 7
        with self.assertRaises(Server429Error):
            SnapchatClient('cid', 'csec', 'rtok', 300).request('GET', path='organizations')
        self.assertEqual(mock_request.call_count, 8)
