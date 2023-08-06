from functools import lru_cache

from app.configuretion.settings import ConsumerKafkaSettings


@lru_cache
def get_consumer_kafka_settings() -> ConsumerKafkaSettings:
    return ConsumerKafkaSettings()
