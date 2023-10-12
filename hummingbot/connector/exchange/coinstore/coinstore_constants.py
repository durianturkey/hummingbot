from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "coinstore"

REST_URL = "https://api.coinstore.com/api"

WSS_URL = "wss://ws.coinstore.com/s/ws"

EXCHANGE_INFO_PATH_URL = "/v2/public/config/spot/symbols"
ACCOUNTS_PATH_URL = "/spot/accountList"
SNAPSHOT_PATH_URL = "/v1/market/depth"
ORDER_PATH_URL = "/trade/order/place"
CANCEL_PATH_URL = "/trade/order/cancel"
TICKER_PRICE_CHANGE_PATH_URL = "/v1/ticker/price"
MY_TRADES_PATH_URL = "/trade/match/accountMatches"
ORDER_INFO_URL = "/trade/order/orderInfo"
SYMBOL_INFO_URL = "/v3/public/assets"

RATE_LIMITS = [
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ORDER_INFO_URL, limit=10, time_interval=1),
    RateLimit(limit_id=SYMBOL_INFO_URL, limit=10, time_interval=1),
]

TRADE_EVENT_TYPE = "trade"
DIFF_EVENT_TYPE = "depth"

WS_HEARTBEAT_TIME_INTERVAL = 30
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 30
WS_AUTHENTICATE_USER_ENDPOINT_NAME = "login"
WS_SUBSCRIPTION_ACCOUNT_CHANNEL_NAME = "!@account"
WS_SUBSCRIPTION_ORDER_CHANNEL_NAME = "!@order"

HBOT_ORDER_ID_PREFIX = ""
MAX_ORDER_ID_LEN = 32

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_STATE = {
    "NOT_FOUND": OrderState.FAILED,
    "SUBMITTING": OrderState.PENDING_CREATE,
    "SUBMITTED": OrderState.OPEN,
    "PARTIAL_FILLED": OrderState.PARTIALLY_FILLED,
    "CANCELED": OrderState.CANCELED,
    "FILLED": OrderState.FILLED,
    "REJECTED": OrderState.FAILED,
}
