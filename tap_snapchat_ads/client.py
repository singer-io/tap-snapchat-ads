from decimal import Decimal
from datetime import datetime, timedelta
import time
import backoff
import requests
from requests.exceptions import ConnectionError, Timeout
import singer
from singer import metrics


API_URL = 'https://adsapi.snapchat.com'
API_VERSION = 'v1'
SNAPCHAT_TOKEN_URL = 'https://accounts.snapchat.com/login/oauth2/access_token'
REQUEST_TIMEOUT = 300 # 5 minutes default timeout
LOGGER = singer.get_logger()

class Server5xxError(Exception):
    pass

class Server429Error(Exception):
    pass


class SnapchatError(Exception):
    pass


class SnapchatBadRequestError(SnapchatError):
    pass


class SnapchatUnauthorizedError(SnapchatError):
    pass


class SnapchatNotFoundError(SnapchatError):
    pass


class SnapchatMethodNotAllowedError(SnapchatError):
    pass


class SnapchatNotAcceptableError(SnapchatError):
    pass


class SnapchatGoneError(SnapchatError):
    pass


class SnapchatTeapotError(SnapchatError):
    pass


class SnapchatForbiddenError(SnapchatError):
    pass


class SnapchatInternalServiceError(Server5xxError):
    pass

class SnapchatServiceUnavailableError(Server5xxError):
    pass


# Error Codes: https://developers.snapchat.com/api/docs/#errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": SnapchatBadRequestError, 
        "message":"The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": SnapchatUnauthorizedError, 
        "message":"Unauthorized access for the URL."
    },
    403: {
        "raise_exception": SnapchatForbiddenError, 
        "message":"User does not have permission to access the resource."
    },
    404: {
        "raise_exception": SnapchatNotFoundError, 
        "message":"The resource you have specified cannot be found."
    },
    405: {
        "raise_exception": SnapchatMethodNotAllowedError, 
        "message":"The provided HTTP method is not supported by the URL."
    },
    406: {
        "raise_exception": SnapchatNotAcceptableError, 
        "message":"You requested a format that isn’t json."
    },
    410: {
        "raise_exception": SnapchatGoneError, 
        "message":"Access to the Snapchat is no longer available."
    },
    418: {
        "raise_exception": SnapchatTeapotError, 
        "message":"The server refuses to brew coffee because it is, permanently, a teapot."
    },
    429: {
        "raise_exception": Server429Error,
        "message": "You are requesting to many requests."
    },
    500: {
        "raise_exception": SnapchatInternalServiceError,
        "message": "An error has occurred at Snapchat's end."
    },
    503: {
        "raise_exception": SnapchatServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}



def get_exception_for_error_code(status_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get("raise_exception", SnapchatError)

# Error message example:
# {
#   "request_status": "ERROR",
#   "request_id": "5ebc40...",
#   "debug_message": "Resource can not be found",
#   "display_message": "We're sorry, but the requested resource is not available at this time",
#   "error_code": "E3003"
# }
def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            status_code = response.status_code
            # get json response if present, some status codes does not contains json response, thus set to empty if not found
            try:
                response_json = response.json()
            except Exception:
                response_json = {}

            error_code = response_json.get('error_code', "")
            if error_code:
                error_code = ", " + error_code
            debug_message = response_json.get('debug_message', response_json.get('error_description', ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get("message", "Unknown Error")))
            error_message = '{}{}: {}'.format(status_code, error_code, debug_message)
            LOGGER.error(error_message)
            if status_code > 500 and status_code != 503:
                exception = Server5xxError
            else:
                exception = get_exception_for_error_code(status_code)
            raise exception(error_message) from error
        except (ValueError, TypeError) as err:
            raise SnapchatError(err) from err

class SnapchatClient: # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 client_id,
                 client_secret,
                 refresh_token,
                 request_timeout,
                 user_agent=None):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__user_agent = user_agent
        self.__access_token = None
        self.__expires = None
        self.__session = requests.Session()
        self.base_url = '{}/{}'.format(API_URL, API_VERSION)


        # if request_timeout is other than 0, "0" or "" then use request_timeout
        if request_timeout and float(request_timeout):
            self.request_timeout = float(request_timeout)
        else: # If value is 0, "0" or "" then set the default which is 300 seconds.
            self.request_timeout = REQUEST_TIMEOUT

    @backoff.on_exception(backoff.expo,
                          (ConnectionError, Timeout, Server5xxError, Server429Error),
                          max_tries=7,
                          factor=3)
    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    def get_access_token(self):
        # The refresh_token never expires and may be used many times to generate each access_token
        # Since the refresh_token does not expire, it is not included in get access_token response
        if self.__access_token is not None and self.__expires > datetime.utcnow():
            return

        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent

        response = self.__session.post(
            url=SNAPCHAT_TOKEN_URL,
            timeout=self.request_timeout, # timeout in seconds
            headers=headers,
            data={
                'grant_type': 'refresh_token',
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
            })

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.__access_token = data.get('access_token')
        expires_in = int(data.get('expires_in', '3600'))
        self.__expires = datetime.utcnow() + timedelta(seconds=expires_in)
        LOGGER.info('Authorized, token expires = {}'.format(self.__expires))


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error, Timeout),
                          max_tries=7,
                          factor=3)
    def request(self, method, path=None, url=None, **kwargs):

        self.get_access_token()

        if not url and self.base_url is None:
            self.base_url = '{}/{}'.format(API_URL, API_VERSION)

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        # endpoint = stream_name (from sync.py API call)
        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        # Rate limits: https://developers.snapchat.com/api/docs/#rate-limits
        # Use retry functionality in backoff to wait and retry if
        # response code equals 429 because rate limit has been exceeded
        # LOGGER.info('headers = {}'.format(response.headers))
        rate_limit = int(response.headers.get('X-Rate-Limit-Limit', 0))
        rate_limit_remaining = int(response.headers.get('X-Rate-Limit-Remaining', 0))
        rate_limit_reset = int(response.headers.get('X-Rate-Limit-Reset', 0))

        if rate_limit == 0:
            rate_limit_percent_remaining = 100
        else:
            rate_limit_percent_remaining = 100 * (Decimal(
                rate_limit_remaining) / Decimal(rate_limit))

        # Wait for reset if remaining calls are less than 5%
        if rate_limit_percent_remaining < 5:
            LOGGER.warning('Rate Limit Warning: {}; remaining calls: {}; remaining %: {}% '.format(
                rate_limit, rate_limit_remaining, int(rate_limit_percent_remaining)))
            wait_time = rate_limit_reset - int(time.time())
            LOGGER.warning('Waiting for {} seconds.'.format(wait_time))
            time.sleep(int(wait_time))

        if response.status_code != 200:
            LOGGER.error('{}: {}'.format(response.status_code, response.text))
            raise_for_error(response)

        # Catch invalid json response
        try:
            response_json = response.json()
        except Exception as err:
            LOGGER.error('{}'.format(err))
            LOGGER.error('response.headers = {}'.format(response.headers))
            LOGGER.error('response.reason = {}'.format(response.reason))
            raise

        return response_json

    def get(self, url, **kwargs):
        return self.request('GET', url=url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url=url, **kwargs)
