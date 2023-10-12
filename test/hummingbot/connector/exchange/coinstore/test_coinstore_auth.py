import asyncio
import hashlib
import hmac
import math
from unittest import TestCase
from unittest.mock import MagicMock

from typing_extensions import Awaitable

from hummingbot.connector.exchange.coinstore.coinstore_auth import CoinstoreAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class CoinstoreAuthTests(TestCase):
    def setUp(self) -> None:
        self._api_key = "testApiKey"
        self._secret = "testSecret"

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def parse_params_to_str(self, params):
        url = "?"
        for key, value in params.items():
            url = url + str(key) + "=" + str(value) + "&"
        return url[0:-1]

    def test_rest_authenticate(self):
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        params = {
            "symbol": "LTCBTC",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": 1,
            "price": "0.1",
        }

        auth = CoinstoreAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)
        request = RESTRequest(method=RESTMethod.GET, params=params, is_auth_required=True)
        configured_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        expires = int(now * 1e6)
        expires_key = str(math.floor(expires / 30000))
        expires_key = expires_key.encode("utf-8")
        secret_key = self._secret.encode("utf-8")
        key = hmac.new(secret_key, expires_key, hashlib.sha256).hexdigest()
        key = key.encode("utf-8")

        params = self.parse_params_to_str(params)
        params = params[1:]
        payload = params.encode("utf-8")
        expected_signature = hmac.new(key, payload, hashlib.sha256).hexdigest()
        self.assertEqual(str(int(now * 1e6)), configured_request.headers["X-CS-EXPIRES"])
        self.assertEqual(expected_signature, configured_request.headers["X-CS-SIGN"])
        self.assertEqual(self._api_key, configured_request.headers["X-CS-APIKEY"])
