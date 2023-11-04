import asyncio
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import AsyncMock, patch

from bidict import bidict

import hummingbot.connector.exchange.coinstore.coinstore_constants as CONSTANTS
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.coinstore.coinstore_api_user_stream_data_source import (
    CoinstoreAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.coinstore.coinstore_auth import CoinstoreAuth
from hummingbot.connector.exchange.coinstore.coinstore_exchange import CoinstoreExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer


class CoinstoreUserStreamDataSourceTests(TestCase):
    # the level is required to receive logs from the data source loger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = "com"
        cls.instrument_id = 4

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant()

        auth = CoinstoreAuth(api_key="TEST_API_KEY", secret_key="TEST_SECRET", time_provider=TimeSynchronizer())

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = CoinstoreExchange(
            client_config_map=client_config_map,
            coinstore_api_key="",
            coinstore_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain,
        )

        self.data_source = CoinstoreAPIUserStreamDataSource(
            auth=auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
        )
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.mocking_assistant = NetworkMockingAssistant()

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _authentication_response(self, authenticated: bool) -> str:
        message = {
            "S": 3,
            "T": "req",
            "sid": "8d0faf40-85ae-067f-4c29-d8f4723b7b05",
            "C": 200 if authenticated else 503,
            "M": "login.success" if authenticated else "auth.session.needed",
        }

        return json.dumps(message)

    def _subscription_response(self, subscription: str) -> str:
        message = {
            "S": 4,
            "T": "req",
            "sid": "5c7a142d-edb5-5343-d82d-666ed9c67ca2",
            "C": 200,
            "M": "sub.channel.success",
            "echo": {"chName": subscription, "startEpochMill": 1695111633294, "msgCount": 30783},
        }

        return json.dumps(message)

    def _raise_exception(self, exception_class):
        raise exception_class

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listening_process_authenticates_and_subscribes_to_events(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        initial_last_recv_time = self.data_source.last_recv_time

        # Add the authentication response for the websocket
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value, self._authentication_response(True)
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._subscription_response(CONSTANTS.WS_SUBSCRIPTION_ACCOUNT_CHANNEL_NAME),
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._subscription_response(CONSTANTS.WS_SUBSCRIPTION_ORDER_CHANNEL_NAME),
        )

        self.listening_task = asyncio.get_event_loop().create_task(self.data_source.listen_for_user_stream(messages))
        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        self.assertTrue(self._is_logged("INFO", "Subscribed to private account, position and orders channels..."))

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(ws_connect_mock.return_value)
        self.assertEqual(2, len(sent_messages))
        request = sent_messages[0]

        self.assertEqual(CONSTANTS.WS_AUTHENTICATE_USER_ENDPOINT_NAME, request["op"])

        expected_payload = {
            "op": "login",
            "auth": [
                {
                    "token": "TEST_API_KEY",
                    "type": "apikey",
                    "expires": 1696037605166,
                    "signature": "0884a5db8c575397e8d5f03e2f79091e64a6518e850d963b00b3946a2d6ba165",  # noqa: mock signature
                }
            ],
            "channel": ["!@account", "!@order"],
        }
        self.assertEqual(expected_payload["channel"], request["channel"])

        self.assertGreater(self.data_source.last_recv_time, initial_last_recv_time)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_authentication_failure(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value, self._authentication_response(False)
        )
        self.listening_task = asyncio.get_event_loop().create_task(self.data_source.listen_for_user_stream(messages))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        self.assertTrue(self._is_logged("ERROR", "Error authenticating the private websocket connection"))
        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_does_not_queue_empty_payload(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, self._authentication_response(True))
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, "")

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_connection_failed(self, mock_ws):
        mock_ws.side_effect = lambda *arg, **kwars: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR.")
        )

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listening_process_canceled_on_cancel_exception(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = asyncio.get_event_loop().create_task(
                self.data_source.listen_for_user_stream(messages)
            )
            self.async_run_with_timeout(self.listening_task)
