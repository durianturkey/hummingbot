import binascii
import hashlib
import hmac
import math
import time
from typing import Any, Dict

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class CoinstoreAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.get_sign(request.params, request.data, request.method))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated.
        """
        return request  # pass-through

    def get_ws_auth_payload(self) -> Dict[str, Any]:
        """
        Generates a dictionary with all required information for the authentication process
        :return: a dictionary of authentication info including the request signature
        """
        secret = self.secret_key
        timeMillis = int(time.time() * 1000)  # Get current timestamp in milliseconds
        payload = str(timeMillis)
        currTime = str(int(timeMillis / 30_000))

        hash = hmac.new(bytes(secret, "utf-8"), bytes(currTime, "utf-8"), hashlib.sha256).digest()
        key = binascii.hexlify(hash).decode()

        hash = hmac.new(bytes(key, "utf-8"), bytes(payload, "utf-8"), hashlib.sha256).digest()
        sign = binascii.hexlify(hash).decode()

        auth_info = {"token": self.api_key, "type": "apikey", "expires": timeMillis, "signature": sign}
        return auth_info

    def parse_params_to_str(self, params):
        url = "?"
        for key, value in params.items():
            url = url + str(key) + "=" + str(value) + "&"
        return url[0:-1]

    def get_sign(self, params, data, method):
        expires = int(self.time_provider.time() * 1e6)
        expires_key = str(math.floor(expires / 30000))
        expires_key = expires_key.encode("utf-8")
        secret_key = self.secret_key.encode("utf-8")
        key = hmac.new(secret_key, expires_key, hashlib.sha256).hexdigest()
        key = key.encode("utf-8")

        if method == RESTMethod.GET:
            params = self.parse_params_to_str(params)
            params = params[1:]
        else:
            params = data

        payload = params.encode("utf-8")
        signature = hmac.new(key, payload, hashlib.sha256).hexdigest()

        return {
            "X-CS-APIKEY": self.api_key,
            "X-CS-EXPIRES": str(expires),
            "X-CS-SIGN": signature,
            "Content-Type": "application/json",
        }
