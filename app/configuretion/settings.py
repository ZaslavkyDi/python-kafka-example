from collections.abc import Callable
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class _BaseKafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
    )
    bootstrap_servers: str = Field(..., description="mybroker1,mybroker2")


class ConsumerKafkaSettings(_BaseKafkaSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix="consumer_"
    )
    auto_offset_reset: str = "earliest"


class ProducerKafkaSettings(_BaseKafkaSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix="producer_"
    )
    max_batch_size: int = 5

    @staticmethod
    def get_key_serializer() -> Callable[[str], bytes]:
        return lambda x: x.encode("utf-8") if x else None

    @staticmethod
    def get_value_serializer() -> Callable[[str], bytes]:
        return lambda x: x.encode("utf-8") if x else None
