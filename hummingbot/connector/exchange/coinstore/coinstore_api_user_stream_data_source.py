import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS
from hummingbot.connector.exchange.coinstore.coinstore_auth import CoinstoreAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinstore.coinstore_exchange import CoinstoreExchange


class CoinstoreAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: CoinstoreAuth,
        trading_pairs: List[str],
        connector: "CoinstoreExchange",
        api_factory: WebAssistantsFactory,
        domain: str = None,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._pong_response_event = None

    async def _authenticate(self, ws: WSAssistant):
        """
        Authenticates user to websocket
        """
        auth_payload = self._auth.get_ws_auth_payload()
        payload = {"op": "login", "auth": auth_payload, "channel": ["!@account", "!@order"]}
        login_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(login_request)
        response: WSResponse = await ws.receive()
        message = response.data

        if message["M"] != "login.success" and message["C"] != 200:
            self.logger().error("Error authenticating the private websocket connection")
            raise IOError("Private websocket connection authentication failed")

        self.logger().info("Subscribed to private account, position and orders channels...")

    # send pong every 5 mins
    async def _send_pong(self, websocket_assistant: WSAssistant):
        while True:
            pong_response = WSJSONRequest(payload={"op": "pong", "epochMillis": time.time()})
            await websocket_assistant.send(pong_response)
            await asyncio.sleep(300)

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        pass

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL, message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE)
        await self._authenticate(ws)
        # send pong every 5 mins
        loop = asyncio.get_event_loop()
        loop.create_task(self._send_pong(ws))
        if not loop.is_running():
            loop.run_forever()
        return ws
