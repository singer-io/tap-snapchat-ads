import unittest
from unittest import mock
import tap_snapchat_ads
from tap_snapchat_ads import SnapchatClient
import tap_snapchat_ads.client as client
from tap_snapchat_ads.client import requests, raise_for_error, SnapchatError, Server5xxError
import requests as req_lib


class MockedResponse:
    def __init__(self, status_code, *args) -> None:
        self.status_code = status_code
        self.content = "test"
        if args:
            self.debug_message = args[0]
        
    def raise_for_status(self):
        raise requests.HTTPError
    
    def json(self):
        if hasattr(self, "debug_message"):
            return {"request_status": "ERROR", "debug_message": self.debug_message}
        return {"request_status": "ERROR"}
        


@mock.patch('requests.Session.request')
class TestExceptionHandling(unittest.TestCase):
    """
        Test cases to verify error is raised with proper message  for SnapchatClient's request method.
    """
    

    def test_400_error_custom_message(self, mocked_request):
        """
            Test case to verify 400 error message from response
        """
        mocked_request.return_value = MockedResponse(400)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatBadRequestError) as e:
            clx.request('GET')
            
        self.assertEqual(str(e.exception), "400: The request is missing or has a bad parameter.")
    
    def test_400_error_response_message(self, mocked_request):
        """
            Test case to verify 400 error message from response
        """

        mocked_request.return_value = MockedResponse(400, "This mesaage from response 400.")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatBadRequestError) as e:
            clx.request('GET')
            
        self.assertEqual(str(e.exception), "400: This mesaage from response 400.")

    def test_401_error_custom_message(self, mocked_request):
        """
            Test case to verify 401 error message from response
        """

        mocked_request.return_value = MockedResponse(401)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatUnauthorizedError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "401: Unauthorized access for the URL.")

    def test_401_error_response_message(self, mocked_request):
        """
            Test case to verify 401 error message from response
        """

        mocked_request.return_value = MockedResponse(401, "This mesaage from response 401.")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatUnauthorizedError) as e:
            clx.request('GET')
            
        self.assertEqual(str(e.exception), "401: This mesaage from response 401.")
 
    def test_403_error_custom_message(self, mocked_request):
        """
            Test case to verify 403 error message from response
        """

        mocked_request.return_value = MockedResponse(403)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatForbiddenError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "403: User does not have permission to access the resource.")

    def test_403_error_response_message(self, mocked_request):
        """
            Test case to verify 403 error message from response
        """

        mocked_request.return_value = MockedResponse(403, "This mesaage from response 403.")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatForbiddenError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "403: This mesaage from response 403.")
    
    def test_404_error_custom_message(self, mocked_request):
        """
            Test case to verify 404 error message from response
        """
        mocked_request.return_value = MockedResponse(404)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatNotFoundError) as e:
            clx.request('GET')
            
        self.assertEqual(str(e.exception), "404: The resource you have specified cannot be found.")

    def test_404_error_response_message(self, mocked_request):
        """
            Test case to verify 404 error message from response
        """

        mocked_request.return_value = MockedResponse(404, 'This mesaage from response 404')
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatNotFoundError) as e:
            clx.request('GET')
        
        self.assertEqual(str(e.exception), "404: This mesaage from response 404")

    def test_405_error_custom_message(self, mocked_request):
        """
            Test case to verify 405 error message from response
        """
        mocked_request.return_value = MockedResponse(405)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatMethodNotAllowedError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "405: The provided HTTP method is not supported by the URL.")

    def test_405_error_response_message(self, mocked_request):
        """
            Test case to verify 405 error message from response
        """

        mocked_request.return_value = MockedResponse(405, "This mesaage from response 405")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatMethodNotAllowedError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "405: This mesaage from response 405")

    def test_406_error_custom_message(self, mocked_request):
        """
            Test case to verify 406 error message from response
        """
        mocked_request.return_value = MockedResponse(406)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatNotAcceptableError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "406: You requested a format that isn’t json.")

    def test_406_error_response_message(self, mocked_request):
        """
            Test case to verify 406 error message from response
        """

        mocked_request.return_value = MockedResponse(406, "This mesaage from response 406")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatNotAcceptableError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "406: This mesaage from response 406")
        
    def test_410_error_custom_message(self, mocked_request):
        """
            Test case to verify 410 error message from response
        """
        mocked_request.return_value = MockedResponse(410)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatGoneError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "410: Access to the Snapchat is no longer available.")

    def test_410_error_response_message(self, mocked_request):
        """
            Test case to verify 410 error message from response
        """

        mocked_request.return_value = MockedResponse(410, "This mesaage from response 410")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatGoneError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "410: This mesaage from response 410")
        
    def test_418_error_custom_message(self, mocked_request):
        """
            Test case to verify 418 error message from response
        """
        mocked_request.return_value = MockedResponse(418)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatTeapotError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "418: The server refuses to brew coffee because it is, permanently, a teapot.")

    def test_418_error_response_message(self, mocked_request):
        """
            Test case to verify 418 error message from response
        """

        mocked_request.return_value = MockedResponse(418, "This mesaage from response 418")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.SnapchatTeapotError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "418: This mesaage from response 418")
        
        

    @mock.patch("time.sleep")   
    def test_500_error_custom_message(self, mocked_time, mocked_request):
        """
            Test case to verify 500 error message from response
        """
        mocked_request.return_value = MockedResponse(500)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.Server5xxError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "500: An error has occurred at Snapchat's end.")

    @mock.patch("time.sleep")   
    def test_500_error_response_message(self, mocked_time, mocked_request):
        """
            Test case to verify 500 error message from response
        """

        mocked_request.return_value = MockedResponse(500, "This mesaage from response 500")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.Server5xxError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "500: This mesaage from response 500")
        
        
    @mock.patch("time.sleep")   
    def test_503_error_custom_message(self, mocked_time, mocked_request):
        """
            Test case to verify 503 error message from response
        """
        mocked_request.return_value = MockedResponse(503)
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.Server5xxError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "503: API service is currently unavailable.")

    @mock.patch("time.sleep")   
    def test_503_error_response_message(self, mocked_time, mocked_request):
        """
            Test case to verify 503 error message from response
        """

        mocked_request.return_value = MockedResponse(503, "This mesaage from response 503")
        clx = SnapchatClient("test", "test", "test", 300)
        with self.assertRaises(client.Server5xxError) as e:
            clx.request('GET')

        self.assertEqual(str(e.exception), "503: This mesaage from response 503")


# ---------------------------------------------------------------------------
# Shared helpers for additional client tests
# ---------------------------------------------------------------------------

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

    def test_json_parse_error_uses_default_message(self):
        resp = self._bad_resp(400, json_raises=True)
        with self.assertRaises(client.SnapchatBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn('400', str(ctx.exception))

    def test_error_code_in_response_gets_comma_prefix(self):
        resp = self._bad_resp(400, json_body={'error_code': 'E1234', 'debug_message': 'bad req'})
        with self.assertRaises(client.SnapchatBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn('E1234', str(ctx.exception))

    def test_status_501_uses_server5xx_generic(self):
        resp = self._bad_resp(501)
        with self.assertRaises(Server5xxError):
            raise_for_error(resp)

    def test_status_504_uses_server5xx_generic(self):
        resp = self._bad_resp(504)
        with self.assertRaises(Server5xxError):
            raise_for_error(resp)

    def test_type_error_in_raise_for_error_becomes_snapchat_error(self):
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
# get_access_token success path + caching
# ---------------------------------------------------------------------------

class TestGetAccessToken(unittest.TestCase):
    @mock.patch('requests.Session.request')
    def test_sets_access_token_and_expires(self, mock_request):
        mock_request.return_value = _make_token_response('mytoken', 1800)
        clx = SnapchatClient('cid', 'csec', 'rtok', 300, user_agent='test-agent')
        clx.get_access_token()
        self.assertEqual(clx._SnapchatClient__access_token, 'mytoken')

    @mock.patch('requests.Session.request')
    def test_cached_token_not_refreshed(self, mock_request):
        mock_request.return_value = _make_token_response('tok1', 3600)
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        clx.get_access_token()
        call_count_after_first = mock_request.call_count
        clx.get_access_token()
        self.assertEqual(mock_request.call_count, call_count_after_first)


# ---------------------------------------------------------------------------
# SnapchatClient context manager (__enter__ / __exit__)
# ---------------------------------------------------------------------------

class TestSnapchatClientContextManager(unittest.TestCase):
    @mock.patch('requests.Session.request')
    def test_enter_returns_self_and_exit_closes_session(self, mock_request):
        mock_request.return_value = _make_token_response()
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        result = clx.__enter__()
        self.assertIs(result, clx)
        clx.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# request() success paths, get(), post()
# ---------------------------------------------------------------------------

class TestRequestMethod(unittest.TestCase):
    @mock.patch('requests.Session.request')
    def test_successful_request_returns_json(self, mock_request):
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'result': 'ok'}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.request('GET', path='organizations')
        self.assertEqual(data, {'result': 'ok'})

    @mock.patch('requests.Session.request')
    def test_get_method(self, mock_request):
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'items': []}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.get(url='https://test.com/v1/organizations')
        self.assertIsInstance(data, dict)

    @mock.patch('requests.Session.request')
    def test_post_method(self, mock_request):
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'created': True}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.post(url='https://test.com/v1/resource')
        self.assertIsInstance(data, dict)

    @mock.patch('requests.Session.request')
    def test_request_with_post_and_user_agent(self, mock_request):
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
        rate_resp = _make_200_response({'rate': 'limited'})
        import time as time_module
        rate_resp.headers = {
            'X-Rate-Limit-Limit': '100',
            'X-Rate-Limit-Remaining': '4',
            'X-Rate-Limit-Reset': str(int(time_module.time()) + 10),
        }
        mock_request.side_effect = [_make_token_response(), rate_resp]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        clx.request('GET', path='organizations')
        mock_sleep.assert_called_once()

    @mock.patch('requests.Session.request')
    def test_invalid_json_response_raises(self, mock_request):
        bad_resp = mock.MagicMock()
        bad_resp.status_code = 200
        bad_resp.headers = {'X-Rate-Limit-Limit': '0', 'X-Rate-Limit-Remaining': '100',
                            'X-Rate-Limit-Reset': '0'}
        bad_resp.json.side_effect = ValueError('bad json')
        bad_resp.text = ''
        mock_request.side_effect = [_make_token_response(), bad_resp]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        with self.assertRaises(ValueError):
            clx.request('GET', path='test')

    @mock.patch('requests.Session.request')
    def test_request_rebuilds_base_url_when_none(self, mock_request):
        mock_request.side_effect = [_make_token_response(), _make_200_response({})]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        clx.base_url = None
        data = clx.request('GET', path='test_endpoint')
        self.assertIsInstance(data, dict)

    @mock.patch('requests.Session.request')
    def test_non_200_api_response_raises(self, mock_request):
        error_resp = mock.MagicMock()
        error_resp.status_code = 400
        error_resp.headers = {'X-Rate-Limit-Limit': '0', 'X-Rate-Limit-Remaining': '0',
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
        mock_request.side_effect = [
            _make_token_response(),
            _make_200_response({'ep': 'test'}),
        ]
        clx = SnapchatClient('cid', 'csec', 'rtok', 300)
        data = clx.request('GET', url='https://test.com', endpoint='organizations')
        self.assertEqual(data.get('ep'), 'test')