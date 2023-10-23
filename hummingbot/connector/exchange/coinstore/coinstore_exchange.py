import asyncio
import math
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinstore import (
    coinstore_constants as CONSTANTS,
    coinstore_utils,
    coinstore_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinstore.coinstore_api_order_book_data_source import CoinstoreAPIOrderBookDataSource
from hummingbot.connector.exchange.coinstore.coinstore_api_user_stream_data_source import (
    CoinstoreAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.coinstore.coinstore_auth import CoinstoreAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class CoinstoreExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        coinstore_api_key: str,
        coinstore_api_secret: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = coinstore_api_key
        self.secret_key = coinstore_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_coinstore_timestamp = 1.0
        self._symbolId_map = bidict()
        super().__init__(client_config_map)

    @staticmethod
    def coinstore_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(coinstore_type: str) -> OrderType:
        return OrderType[coinstore_type]

    @property
    def authenticator(self):
        return CoinstoreAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "coinstore"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_symbol_request_path(self):
        return CONSTANTS.SYMBOL_INFO_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        return pairs_prices

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = (
            "-1021" in error_description and "Timestamp for this request" in error_description
        )
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler, time_synchronizer=self._time_synchronizer, domain=self._domain, auth=self._auth
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinstoreAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinstoreAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        type_str = CoinstoreExchange.coinstore_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "side": side_str,
            "ordType": type_str,
            "ordQty": amount_str,
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "clOrdId": order_id,
        }
        if order_type is OrderType.LIMIT:
            price_str = f"{price:.4f}"
            api_params["ordPrice"] = price_str

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_PATH_URL, is_auth_required=True, data=api_params
            )
            o_id = str(order_result["data"]["ordId"])
            transact_time = time.time() * 1e-3
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = (
                "status is 503" in error_description
                and "Unknown error, please check your request or try again later." in error_description
            )
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "symbol": symbol,
            "ordId": order_id,
        }
        cancel_result = await self._api_post(path_url=CONSTANTS.CANCEL_PATH_URL, data=api_params, is_auth_required=True)
        if cancel_result.get("data").get("state") == "CANCELED":
            return True
        return False

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        cancelled = await self._place_cancel(order.exchange_order_id, order)
        if cancelled:
            update_timestamp = self.current_timestamp
            if update_timestamp is None or math.isnan(update_timestamp):
                update_timestamp = self._time()
            order_update: OrderUpdate = OrderUpdate(
                client_order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                update_timestamp=update_timestamp,
                new_state=(
                    OrderState.CANCELED if self.is_cancel_request_in_exchange_synchronous else OrderState.PENDING_CANCEL
                ),
            )
            self._order_tracker.process_order_update(order_update)
        return cancelled

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {
            "data": {
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
        }
        """
        trading_pair_rules = exchange_info_dict.get("data", [])
        retval = []
        for rule in filter(coinstore_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                    symbol=rule.get("symbolCode").upper()
                )

                min_order_size = Decimal(rule.get("minLmtSz"))
                # tick_size = rule.get("tickSz")
                # step_size = Decimal(rule.get("lotSz"))
                min_notional = Decimal(rule.get("minLmtPr"))

                retval.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=min_notional,
                        min_base_amount_increment=min_order_size,
                        min_notional_size=min_notional,
                    )
                )

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                if event_message.get("ordId"):
                    execution_type = event_message.get("ordState")
                    client_order_id = str(event_message.get("ordId"))
                    if execution_type == "PARTIAL_FILLED" or execution_type == "FILLED":
                        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                        if tracked_order is not None:
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=self._get_symbol(event_message["feeCurrencyId"]),
                                flat_fees=[
                                    TokenAmount(
                                        amount=Decimal(event_message["fee"]),
                                        token=self._get_symbol(event_message["feeCurrencyId"]),
                                    )
                                ],
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(event_message["tradeId"]),
                                client_order_id=client_order_id,
                                exchange_order_id=str(event_message["ordId"]),
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(event_message["execQty"]),
                                fill_quote_amount=Decimal(event_message["execQty"])
                                * Decimal(event_message["ordPrice"]),
                                fill_price=Decimal(event_message["ordPrice"]),
                                fill_timestamp=event_message["timestamp"] * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)

                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message["timestamp"] * 1e-3,
                            new_state=CONSTANTS.ORDER_STATE[event_message["ordState"]],
                            client_order_id=client_order_id,
                            exchange_order_id=str(event_message["ordId"]),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)
                elif event_message.get("frozen"):
                    asset_name = event_message["currency"]
                    free_balance = Decimal(event_message["available"])
                    total_balance = Decimal(event_message["available"]) + Decimal(event_message["frozen"])
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup measure to get filled events with trade ID for orders,
        in case Coinstore's user stream events are not working.
        NOTE: It is not required to copy this functionality in other connectors.
        This is separated from _update_order_status which only updates the order status without producing filled
        events, since Coinstore's get order endpoint does not return trade IDs.
        The minimum poll interval for order status is 10 seconds.
        """
        small_interval_last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        small_interval_current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        long_interval_last_tick = self._last_poll_timestamp / self.LONG_POLL_INTERVAL
        long_interval_current_tick = self.current_timestamp / self.LONG_POLL_INTERVAL

        if long_interval_current_tick > long_interval_last_tick or (
            self.in_flight_orders and small_interval_current_tick > small_interval_last_tick
        ):
            query_time = int(self._last_trades_poll_coinstore_timestamp * 1e3)
            self._last_trades_poll_coinstore_timestamp = self._time_synchronizer.time()
            order_by_exchange_id_map = {}
            for order in self._order_tracker.all_fillable_orders.values():
                order_by_exchange_id_map[order.exchange_order_id] = order

            tasks = []
            trading_pairs = self.trading_pairs
            for trading_pair in trading_pairs:
                params = {"symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)}
                if self._last_poll_timestamp > 0:
                    params["startTime"] = query_time
                tasks.append(self._api_get(path_url=CONSTANTS.MY_TRADES_PATH_URL, params=params, is_auth_required=True))

            self.logger().debug(f"Polling for order fills of {len(tasks)} trading pairs.")
            results = await safe_gather(*tasks, return_exceptions=True)

            for trades, trading_pair in zip(results, trading_pairs):
                if isinstance(trades, Exception):
                    self.logger().network(
                        f"Error fetching trades update for the order {trading_pair}: {trades}.",
                        app_warning_msg=f"Failed to fetch trade update for {trading_pair}.",
                    )
                    continue
                if len(trades) == 0:
                    return
                if len(trades["data"]) == 0:
                    return
                for trade in trades["data"]:
                    exchange_order_id = str(trade["orderId"])
                    if exchange_order_id in order_by_exchange_id_map:
                        # This is a fill for a tracked order
                        tracked_order = order_by_exchange_id_map[exchange_order_id]
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=tracked_order.trade_type,
                            percent_token=self._get_symbol(trade["feeCurrencyId"]),
                            flat_fees=[
                                TokenAmount(
                                    amount=Decimal(trade["fee"]), token=self._get_symbol(trade["feeCurrencyId"])
                                )
                            ],
                        )
                        price = Decimal(trade["execAmt"]) / Decimal(trade["execQty"])
                        trade_update = TradeUpdate(
                            trade_id=str(trade["id"]),
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(trade["execQty"]),
                            fill_quote_amount=Decimal(trade["execAmt"]) / price,
                            fill_price=price,
                            fill_timestamp=trade["matchTime"] * 1e-3,
                        )
                        self._order_tracker.process_trade_update(trade_update)
                    elif self.is_confirmed_new_order_filled_event(str(trade["id"]), exchange_order_id, trading_pair):
                        # This is a fill of an order registered in the DB but not tracked any more
                        self._current_trade_fills.add(
                            TradeFillOrderDetails(
                                market=self.display_name, exchange_trade_id=str(trade["id"]), symbol=trading_pair
                            )
                        )
                        self.trigger_event(
                            MarketEvent.OrderFilled,
                            OrderFilledEvent(
                                timestamp=float(trade["matchTime"]) * 1e-3,
                                order_id=self._exchange_order_ids.get(str(trade["orderId"]), None),
                                trading_pair=trading_pair,
                                trade_type=TradeType.BUY if trade["side"] else TradeType.SELL,
                                order_type=OrderType.LIMIT if trade["role"] == -1 else OrderType.MARKET,
                                price=Decimal(trade["execAmt"]) / Decimal(trade["execQty"]),
                                amount=Decimal(trade["execQty"]),
                                trade_fee=DeductedFromReturnsTradeFee(
                                    flat_fees=[
                                        TokenAmount(self._get_symbol(trade["feeCurrencyId"]), Decimal(trade["fee"]))
                                    ]
                                ),
                                exchange_trade_id=str(trade["id"]),
                            ),
                        )
                        self.logger().info(f"Recreating missing trade in TradeFill: {trade}")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = int(order.exchange_order_id)
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                params={"symbol": trading_pair, "orderId": exchange_order_id},
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PATH_URL,
            )

            for trade in all_fills_response["data"]:
                exchange_order_id = str(trade["orderId"])
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type="BUY" if order.trade_type == 1 else "SELL",
                    percent_token=self._get_symbol(trade["feeCurrencyId"]),
                    flat_fees=[
                        TokenAmount(amount=Decimal(trade["fee"]), token=self._get_symbol(trade["feeCurrencyId"]))
                    ],
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["execQty"]),
                    fill_quote_amount=Decimal(trade["execAmt"]),
                    fill_price=Decimal(trade["execAmt"]) / Decimal(trade["execQty"]),
                    fill_timestamp=trade["matchTime"] * 1e-3,
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        updated_order = await self._api_get(
            path_url=CONSTANTS.ORDER_INFO_URL,
            params={"ordId": tracked_order.exchange_order_id},
            is_auth_required=True,
        )

        if updated_order.get("message") == "Order Not Found":
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time() * 1e-3,
                new_state=OrderState.CANCELED,
            )
        updated_order_data = updated_order["data"]
        new_state = CONSTANTS.ORDER_STATE[updated_order_data["ordState"]]

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["ordId"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data["timestamp"],
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        new_balances = {}
        self._account_available_balances = {}

        account_info = await self._api_request(
            method=RESTMethod.POST, path_url=CONSTANTS.ACCOUNTS_PATH_URL, is_auth_required=True, params={}, data={}
        )
        balances = account_info["data"]
        for balance_entry in balances:
            asset_name = balance_entry["currency"]
            status = balance_entry["type"]
            balance = Decimal(balance_entry["balance"])
            if status == 1:
                self._account_available_balances[asset_name] = balance
            if asset_name in new_balances:
                new_balances[asset_name] += balance
            else:
                new_balances[asset_name] = balance

        self._account_balances = new_balances

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_post(path_url=self.trading_pairs_request_path, data={})
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_post(path_url=self.trading_pairs_request_path, data={})
        return exchange_info

    async def _make_trading_symbols_request(self) -> Any:
        assets_info = await self._api_get(
            overwrite_url="https://api.coinstore.com/v3/public/assets", path_url="", limit_id=CONSTANTS.SYMBOL_INFO_URL
        )
        return assets_info

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in exchange_info["data"]:
            mapping[symbol_data["symbolCode"].upper()] = combine_to_hb_trading_pair(
                base=symbol_data["tradeCurrencyCode"].upper(), quote=symbol_data["quoteCurrencyCode"]
            )
            self._symbolId_map[symbol_data["symbolCode"].upper()] = symbol_data["symbolId"]
        self._set_trading_pair_symbol_map(mapping)

    def _initialize_assets_info_from_assets_info(self, assets_info: Dict[str, Any]):
        for symbol_data in assets_info["data"]:
            info = assets_info["data"][symbol_data]
            self._symbolId_map[symbol_data.upper()] = info["unified_cryptoasset_id"]

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._make_trading_pairs_request()
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
            assets_info = await self._make_trading_symbols_request()
            self._initialize_assets_info_from_assets_info(assets_info=assets_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _get_symbol_id(self, trading_pair: str) -> int:
        return self._symbolId_map[trading_pair]

    def _get_symbol(self, id: int) -> str:
        return self._symbolId_map.inverse[id]

    def _set_symbolId_map(self, symbolId_map: Optional[Mapping[str, str]]):
        self._symbolId_map = symbolId_map

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {"symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)}

        resp_json = await self._api_request(
            method=RESTMethod.GET, path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL, params=params
        )

        return float(resp_json["data"][0]["price"])
