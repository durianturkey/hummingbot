import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS, coinstore_web_utils as web_utils
from hummingbot.connector.exchange.coinstore.coinstore_exchange import CoinstoreExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import MarketOrderFailureEvent


class CoinstoreExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):
    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL, domain=self.exchange._domain)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL, domain=self.exchange._domain)
        url = f"{url}?symbol={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        url = web_utils.private_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.private_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNTS_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def all_symbols_request_mock_response(self):
        return {
            "code": "0",
            "message": "Success",
            "data": [
                {
                    "symbolId": 1,
                    "symbolCode": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "tradeCurrencyCode": self.base_asset,
                    "quoteCurrencyCode": self.quote_asset,
                    "openTrade": True,
                    "onLineTime": 1609813531019,
                    "tickSz": 0,
                    "lotSz": 4,
                    "minLmtPr": "0.0002",
                    "minLmtSz": "0.2",
                    "minMktVa": "0.1",
                    "minMktSz": "0.1",
                    "makerFee": "0.006",
                    "takerFee": "0.003",
                }
            ],
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "code": 0,
            "message": "",
            "data": [
                {
                    "id": 4,
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "price": str(self.expected_latest_price),
                }
            ],
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "code": "0",
            "message": "Success",
            "data": [
                {
                    "symbolId": 8,
                    "symbolCode": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                    "tradeCurrencyCode": "INVALID",
                    "quoteCurrencyCode": "PAIR",
                    "openTrade": True,
                    "onLineTime": 1609813531019,
                    "tickSz": 0,
                    "lotSz": 4,
                    "minLmtPr": "0.0002",
                    "minLmtSz": "0.2",
                    "minMktVa": "0.1",
                    "minMktSz": "0.1",
                    "makerFee": "0.006",
                    "takerFee": "0.003",
                },
                {
                    "symbolId": 1,
                    "symbolCode": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "tradeCurrencyCode": self.base_asset,
                    "quoteCurrencyCode": self.quote_asset,
                    "openTrade": True,
                    "onLineTime": 1609813531019,
                    "tickSz": 0,
                    "lotSz": 4,
                    "minLmtPr": "0.0002",
                    "minLmtSz": "0.2",
                    "minMktVa": "0.1",
                    "minMktSz": "0.1",
                    "makerFee": "0.006",
                    "takerFee": "0.003",
                },
            ],
        }

        return "INVALID-PAIR", response

    @property
    def network_status_request_successful_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        return self.all_symbols_request_mock_response

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "code": "0",
            "message": "Success",
            "data": [
                {
                    "symbolId": 1,
                    "symbolCode": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "tradeCurrencyCode": self.base_asset,
                    "quoteCurrencyCode": self.quote_asset,
                    "openTrade": True,
                    "onLineTime": 1609813531019,
                    "lotSz": 4,
                    "makerFee": "0.006",
                    "takerFee": "0.003",
                }
            ],
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {"code": "0", "data": {"ordId": self.expected_exchange_order_id}}

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "data": [
                {
                    "uid": 315,
                    "accountId": 1134971,
                    "currency": self.base_asset,
                    "balance": "5.0",
                    "type": 4,
                    "typeName": "FROZEN",
                },
                {
                    "uid": 315,
                    "accountId": 1134971,
                    "currency": self.base_asset,
                    "balance": "10.0",
                    "type": 1,
                    "typeName": "AVAILABLE",
                },
                {
                    "uid": 315,
                    "accountId": 1134972,
                    "currency": self.quote_asset,
                    "balance": "0.00000000",
                    "type": 4,
                    "typeName": "FROZEN",
                },
                {
                    "uid": 315,
                    "accountId": 1134972,
                    "currency": self.quote_asset,
                    "balance": "2000",
                    "type": 1,
                    "typeName": "AVAILABLE",
                },
            ],
            "code": 0,
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "data": [
                {
                    "uid": 315,
                    "accountId": 1134971,
                    "currency": self.base_asset,
                    "balance": "5.0",
                    "type": 4,
                    "typeName": "FROZEN",
                },
                {
                    "uid": 315,
                    "accountId": 1134971,
                    "currency": self.base_asset,
                    "balance": "10.0",
                    "type": 1,
                    "typeName": "AVAILABLE",
                },
            ],
            "code": 0,
        }

    @property
    def balance_event_websocket_update(self):
        return {"accountId": 12, "currency": self.base_asset, "frozen": "5", "available": "10", "timestamp": 1602493840}

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        data = self.trading_rules_request_mock_response["data"][0]
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(data["minLmtSz"]),
            min_price_increment=Decimal(data["minLmtPr"]),
            min_base_amount_increment=Decimal(data["minLmtSz"]),
            min_notional_size=Decimal(data["minLmtPr"]),
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["data"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return 28

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(
            percent_token="USDT", flat_fees=[TokenAmount(token="USDT", amount=Decimal("30"))]
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return str(30000)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset_id = 0
        cls.quote_asset_id = 1

    def setUp(self) -> None:
        super().setUp()
        self.exchange._set_symbolId_map(
            bidict({self.base_asset: self.base_asset_id, self.quote_asset: self.quote_asset_id})
        )

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        return CoinstoreExchange(
            client_config_map=client_config_map,
            coinstore_api_key="testApiKey",
            coinstore_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        self._validate_auth_credentials_taking_parameters_from_argument(
            request_call_tuple=request_call, params=request_call.kwargs["params"] or request_call.kwargs["data"]
        )

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = dict(json.loads(request_call.kwargs["data"]))
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["symbol"])
        self.assertEqual(order.trade_type.name.upper(), request_data["side"])
        self.assertEqual(CoinstoreExchange.coinstore_order_type(OrderType.LIMIT), request_data["ordType"])
        self.assertEqual(Decimal("100"), Decimal(request_data["ordQty"]))
        self.assertEqual(Decimal("10000"), Decimal(request_data["ordPrice"]))
        self.assertEqual(order.client_order_id, request_data["clOrdId"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["symbol"])
        self.assertEqual(order.exchange_order_id, request_data["ordId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(order.exchange_order_id, request_params["ordId"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])
        self.assertEqual(order.exchange_order_id, str(request_params["orderId"]))

    def configure_successful_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.post(regex_url, status=400, callback=callback)
        return url

    def configure_order_not_found_error_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = {"code": -2011, "msg": "Unknown order sent."}
        mock_api.post(regex_url, status=400, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
        self, successful_order: InFlightOrder, erroneous_order: InFlightOrder, mock_api: aioresponses
    ) -> List[str]:
        """
        :return: a list of all configured URLs for the cancelations
        """
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_INFO_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_INFO_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_open_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        """
        :return: the URL configured
        """
        url = web_utils.private_rest_url(CONSTANTS.ORDER_INFO_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_INFO_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_INFO_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_order_not_found_error_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> List[str]:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = {"code": -2013, "msg": "Order does not exist."}
        mock_api.get(regex_url, body=json.dumps(response), status=400, callback=callback)
        return [url]

    def configure_partial_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.MY_TRADES_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_all_symbols_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.all_symbols_url
        response = self.all_symbols_request_mock_response
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return [url]

    def _configure_balance_response(
        self,
        response: Dict[str, Any],
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self.balance_url
        mock_api.post(
            re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?")), body=json.dumps(response), callback=callback
        )
        return url

    def configure_trading_rules_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.trading_rules_url
        response = self.trading_rules_request_mock_response
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return [url]

    def configure_erroneous_trading_rules_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.trading_rules_url
        response = self.trading_rules_request_erroneous_mock_response
        mock_api.post(url, body=json.dumps(response), callback=callback)
        return [url]

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "version": 12,
            "accountId": 1,
            "ordId": 11,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": order.trade_type.name.upper(),
            "ordType": order.order_type.name.upper(),
            "timeInForce": "GTC",
            "ordPrice": str(order.price),
            "ordQty": str(order.amount),
            "ordAmt": str(order.price * order.amount),
            "ordState": "SUBMITTED",
            "execQty": "0.00000000",
            "execAmt": "0.00000000",
            "remainingQty": str(order.amount),
            "matchId": 12,
            "tradeId": 123,
            "matchRole": "MAKER",
            "matchQty": "0.00000000",
            "matchAmt": "0.00000000",
            "selfDealingQty": "0.00000000",
            "actualFeeRate": "0.002",
            "feeCurrencyId": "30",
            "fee": 0,
            "timestamp": 1499405658,
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "version": 12,
            "accountId": 1,
            "ordId": 11,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": order.trade_type.name.upper(),
            "ordType": order.order_type.name.upper(),
            "timeInForce": "GTC",
            "ordPrice": str(order.price),
            "ordQty": str(order.amount),
            "ordAmt": str(order.price * order.amount),
            "ordState": "CANCELED",
            "execQty": "0.00000000",
            "execAmt": "0.00000000",
            "remainingQty": str(order.amount),
            "matchId": 12,
            "tradeId": 123,
            "matchRole": "MAKER",
            "matchQty": "0.00000000",
            "matchAmt": "0.00000000",
            "selfDealingQty": "0.00000000",
            "actualFeeRate": "0.002",
            "feeCurrencyId": "30",
            "fee": 0,
            "timestamp": 1499405658,
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "version": 12,
            "accountId": 1,
            "ordId": 11,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": order.trade_type.name.upper(),
            "ordType": order.order_type.name.upper(),
            "timeInForce": "GTC",
            "ordPrice": str(order.price),
            "ordQty": str(order.amount),
            "ordAmt": str(order.price * order.amount),
            "ordState": "FILLED",
            "execQty": str(order.amount),
            "execAmt": str(order.price * order.amount),
            "remainingQty": 0,
            "matchId": 12,
            "tradeId": 123,
            "matchRole": "MAKER",
            "matchQty": "0.00000000",
            "matchAmt": "0.00000000",
            "selfDealingQty": "0.00000000",
            "actualFeeRate": "0.002",
            "feeCurrencyId": "30",
            "fee": str(self.expected_fill_fee.flat_fees[0].amount),
            "timestamp": 1499405658,
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return None

    def test_user_stream_update_for_order_failure(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="100234",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        event_message = {
            "version": 12,
            "accountId": 1,
            "ordId": order.client_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": order.trade_type.name.upper(),
            "ordType": order.order_type.name.upper(),
            "timeInForce": "GTC",
            "ordPrice": str(order.price),
            "ordQty": str(order.amount),
            "ordAmt": str(order.price * order.amount),
            "ordState": "REJECTED",
            "execQty": "0.00000000",
            "execAmt": "0.00000000",
            "remainingQty": str(order.amount),
            "matchId": 12,
            "tradeId": 123,
            "matchRole": "MAKER",
            "matchQty": "0.00000000",
            "matchAmt": "0.00000000",
            "selfDealingQty": "0.00000000",
            "actualFeeRate": "0.002",
            "feeCurrencyId": "30",
            "fee": 0,
            "timestamp": 1499405658657 / 1e3,
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(order.client_order_id, failure_event.order_id)
        self.assertEqual(order.order_type, failure_event.order_type)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_failure)
        self.assertTrue(order.is_done)

    @patch("hummingbot.connector.utils.get_tracking_nonce")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 7

        result = self.exchange.buy(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_ORDER_ID_PREFIX,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)
        exception = IOError(
            "Error executing request POST https://api.binance.com/api/v3/order. HTTP status is 400. "
            "Error: {'code':-1021,'msg':'Timestamp for this request is outside of the recvWindow.'}"
        )
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

        exception = IOError(
            "Error executing request POST https://api.binance.com/api/v3/order. HTTP status is 400. "
            "Error: {'code':-1021,'msg':'Timestamp for this request was 1000ms ahead of the server's "
            "time.'}"
        )
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

        exception = IOError(
            "Error executing request POST https://api.binance.com/api/v3/order. HTTP status is 400. "
            "Error: {'code':-1022,'msg':'Timestamp for this request was 1000ms ahead of the server's "
            "time.'}"
        )
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

        exception = IOError(
            "Error executing request POST https://api.binance.com/api/v3/order. HTTP status is 400. "
            "Error: {'code':-1021,'msg':'Other error.'}"
        )
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

    @aioresponses()
    def test_place_order_manage_server_overloaded_error_unkown_order(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (
            self.exchange.current_timestamp - self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1
        )
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_response = {"code": -1003, "msg": "Unknown error, please check your request or try again later."}
        mock_api.post(regex_url, body=json.dumps(mock_response), status=503)

        o_id, transact_time = self.async_run_with_timeout(
            self.exchange._place_order(
                order_id="test_order_id",
                trading_pair=self.trading_pair,
                amount=Decimal("1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("2"),
            )
        )
        self.assertEqual(o_id, "UNKNOWN")

    @aioresponses()
    def test_place_order_manage_server_overloaded_error_failure(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = (
            self.exchange.current_timestamp - self.exchange.UPDATE_ORDER_STATUS_MIN_INTERVAL - 1
        )

        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_response = {"code": -1003, "msg": "Service Unavailable."}
        mock_api.post(regex_url, body=json.dumps(mock_response), status=503)

        self.assertRaises(
            IOError,
            self.async_run_with_timeout,
            self.exchange._place_order(
                order_id="test_order_id",
                trading_pair=self.trading_pair,
                amount=Decimal("1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("2"),
            ),
        )

        mock_response = {"code": -1003, "msg": "Internal error; unable to process your request. Please try again."}
        mock_api.post(regex_url, body=json.dumps(mock_response), status=503)

        self.assertRaises(
            IOError,
            self.async_run_with_timeout,
            self.exchange._place_order(
                order_id="test_order_id",
                trading_pair=self.trading_pair,
                amount=Decimal("1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("2"),
            ),
        )

    def test_format_trading_rules(self):
        trading_rules = [
            {
                "permissions": ["SPOT"],
                "symbolId": 2,
                "symbolCode": "COINALPHAHBOT",
                "tradeCurrencyCode": "COINALPHA",
                "quoteCurrencyCode": "HBOT",
                "openTrade": True,
                "onLineTime": 1611104824934,
                "tickSz": 6,
                "lotSz": 2,
                "minLmtPr": "0.001",
                "minLmtSz": "1",
                "minMktVa": "1",
                "minMktSz": "1",
                "makerFee": "0.002",
                "takerFee": "0.002",
            }
        ]
        exchange_info = {"data": trading_rules}

        result = self.async_run_with_timeout(self.exchange._format_trading_rules(exchange_info))

        self.assertEqual(result[0].min_notional_size, Decimal("0.00100000"))

    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        return

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        return

    def _validate_auth_credentials_taking_parameters_from_argument(
        self, request_call_tuple: RequestCall, params: Dict[str, Any]
    ):
        request_headers = request_call_tuple.kwargs["headers"]
        self.assertIn("X-CS-EXPIRES", request_headers)
        self.assertIn("X-CS-SIGN", request_headers)
        self.assertEqual("testApiKey", request_headers["X-CS-APIKEY"])

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "code": 0,
            "data": {
                "clientOrderId": order.client_order_id,
                "state": "CANCELED",
                "ordId": order.exchange_order_id or "dummyOrdId",
            },
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "data": {
                "baseCurrency": self.base_asset,
                "quoteCurrency": self.quote_asset,
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "timestamp": 1499827319559,
                "side": order.trade_type.name.upper(),
                "accountId": 1138204,
                "ordId": order.exchange_order_id,
                "clOrdId": order.client_order_id,
                "ordType": order.order_type.name.upper(),
                "ordState": "FILLED",
                "ordPrice": str(order.price),
                "ordQty": str(order.amount),
                "ordAmt": str(order.price * order.amount),
                "cumAmt": str((order.price + Decimal(2)) * order.price),
                "cumQty": str(order.price + Decimal(2)),
                "leavesQty": "0",
                "avgPrice": "0",
                "feeCurrency": "LUFFY",
                "timeInForce": "GTC",
            },
            "code": 0,
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "data": {
                "baseCurrency": self.base_asset,
                "quoteCurrency": self.quote_asset,
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "timestamp": 1499827319559,
                "side": "BUY",
                "accountId": 1138204,
                "ordId": order.exchange_order_id,
                "clOrdId": order.client_order_id,
                "ordType": "LIMIT",
                "ordState": "CANCELED",
                "ordPrice": "0.00000000092",
                "ordQty": "1",
                "ordAmt": "0.00000000092",
                "cumAmt": "0",
                "cumQty": "0",
                "leavesQty": "0",
                "avgPrice": "0",
                "feeCurrency": "LUFFY",
                "timeInForce": "GTC",
            },
            "code": 0,
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "data": [
                {
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "baseCurrency": self.base_asset,
                    "quoteCurrency": self.quote_asset,
                    "timestamp": 1499827319559,
                    "side": order.trade_type.name.upper(),
                    "timeInForce": "GTC",
                    "accountId": 1138204,
                    "ordPrice": str(order.price),
                    "cumAmt": "0.0",
                    "cumQty": "0.0",
                    "leavesQty": "0",
                    "clOrdId": order.client_order_id,
                    "ordAmt": str(order.price * order.amount),
                    "ordQty": str(order.amount),
                    "ordId": order.exchange_order_id,
                    "ordStatus": "SUBMITTED",
                    "ordType": order.order_type.name.upper(),
                }
            ],
            "code": 0,
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "data": {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "baseCurrency": self.base_asset,
                "quoteCurrency": self.quote_asset,
                "timestamp": 1499827319559,
                "side": order.trade_type.name.upper(),
                "timeInForce": "GTC",
                "accountId": 1138204,
                "ordPrice": str(order.price),
                "cumAmt": str((order.price + Decimal(2)) * order.price),
                "cumQty": str(order.price + Decimal(2)),
                "leavesQty": "0",
                "clOrdId": order.client_order_id,
                "ordAmt": str(order.price * order.amount),
                "ordQty": str(order.amount),
                "ordId": order.exchange_order_id,
                "ordState": "PARTIAL_FILLED",
                "ordType": order.order_type.name.upper(),
            },
            "code": 0,
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder):
        return {
            "data": [
                {
                    "id": self.expected_fill_trade_id,
                    "remainingQty": 0e-18,
                    "matchRole": 1,
                    "feeCurrencyId": "30",
                    "acturalFeeRate": 0.002000000000000000,
                    "role": 1,
                    "accountId": 1138204,
                    "instrumentId": "COINALPHA-HBOT",
                    "baseCurrencyId": self.base_asset_id,
                    "quoteCurrencyId": self.quote_asset_id,
                    "execQty": str(self.expected_partial_fill_amount),
                    "orderState": 50,
                    "matchId": 258338866,
                    "orderId": int(order.exchange_order_id),
                    "side": 1,
                    "execAmt": str(self.expected_partial_fill_amount * self.expected_partial_fill_price),
                    "selfDealingQty": 0e-18,
                    "tradeId": 11523732,
                    "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                    "matchTime": 1640825389,
                    "seq": None,
                }
            ],
            "code": 0,
        }

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder):
        return {
            "data": [
                {
                    "id": self.expected_fill_trade_id,
                    "remainingQty": 0e-18,
                    "matchRole": 1,
                    "feeCurrencyId": "30",
                    "acturalFeeRate": 0.002000000000000000,
                    "role": 1,
                    "accountId": 1138204,
                    "instrumentId": "COINALPHA-HBOT",
                    "baseCurrencyId": self.base_asset_id,
                    "quoteCurrencyId": "COINALPHA",
                    "execQty": str(order.amount),
                    "orderState": 50,
                    "matchId": 258338866,
                    "orderId": int(order.exchange_order_id),
                    "side": 1,
                    "execAmt": str(order.amount * order.price),
                    "selfDealingQty": 0e-18,
                    "tradeId": 11523732,
                    "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                    "matchTime": 1640825389,
                    "seq": None,
                }
            ],
            "code": 0,
        }
