from functools import lru_cache

from app.configuretion.settings import ConsumerKafkaSettings, ProducerKafkaSettings


@lru_cache
def get_consumer_kafka_settings() -> ConsumerKafkaSettings:
    return ConsumerKafkaSettings()


@lru_cache
def get_producer_kafka_settings() -> ProducerKafkaSettings:
    return ProducerKafkaSettings()
