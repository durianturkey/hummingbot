from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

DEFAULT_FEES = TradeFeeSchema(
    maker_fixed_fees=[TokenAmount("USDT", Decimal("1"))],
    taker_fixed_fees=[TokenAmount("USDT", Decimal("1"))],
    buy_percent_fee_deducted_from_returns=True,
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("openTrade")


class CoinstoreConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="coinstore", const=True, client_data=None)
    coinstore_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Coinstore public API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    coinstore_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Coinstore secret API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )

    class Config:
        title = "coinstore"


KEYS = CoinstoreConfigMap.construct()
