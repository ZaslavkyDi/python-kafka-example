from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseKafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
    )
    bootstrap_servers: str = Field(..., description="mybroker1,mybroker2")


class ConsumerKafkaSettings(BaseKafkaSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix="consumer_"
    )
    auto_offset_reset: str = "earliest"
