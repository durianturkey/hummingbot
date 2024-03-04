import hashlib
import hmac
import json
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class CoinsbitAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider # not used in coinsbit
    
    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        https://github.com/Coinsbit-connect/api/wiki/API#http-authorization
        """
        if request.method == RESTMethod.POST:
            request.data = self.add_auth_to_params(params=json.loads(request.data))
        else:
            request.params = self.add_auth_to_params(params=request.params)
        
        self.add_auth_to_params(request.headers)

        return request

    def add_auth_to_params(self, params: dict) -> dict:
        payload = "meow"
        # hmac sign the payload and output in hex
        signature = hmac.new(self.secret_key.encode(), payload.encode(), hashlib.sha512).hexdigest()
        params.update({
            "Content-type": "application/json",
            "X-TXC-APIKEY": self.api_key,
            "X-TXC-PAYLOAD": payload,
            "X-TXC-SIGNATURE": signature
        })
        return params