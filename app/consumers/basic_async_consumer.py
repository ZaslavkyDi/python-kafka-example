import abc
import logging

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from app.configuretion import get_consumer_kafka_settings

logger = logging.getLogger(__name__)


class BaseAsyncKafkaConsumer(metaclass=abc.ABCMeta):

    def __init__(self, topics: list[str], group_id: str) -> None:
        self._topics = topics
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    def _build_aio_consumer(self) -> AIOKafkaConsumer:
        if self._consumer:
            return self._consumer

        self._consumer = AIOKafkaConsumer(
            *self._topics,
            group_id=self._group_id,
            enable_auto_commit=False,
            bootstrap_servers=get_consumer_kafka_settings().bootstrap_servers,
            auto_offset_reset=get_consumer_kafka_settings().auto_offset_reset,
            value_deserializer=lambda x: x.decode('utf-8'),
        )
        return self._consumer

    async def start_consumer(self) -> None:
        consumer = self._build_aio_consumer()
        print("Consumer created")

        await consumer.start()

        print("start listen for messages")
        async for message in consumer:
            try:
                await self._process_message(kafka_message=message)
                await consumer.commit()
            except Exception as e:
                logger.error(str(e))
                await self._publish_message_to_failed_topic(
                    kafka_message=message,
                    error=e,
                )

    async def stop_consumer(self) -> None:
        if self._consumer:
            await self._consumer.stop()

        self._consumer = None

    @abc.abstractmethod
    async def _process_message(self, kafka_message: ConsumerRecord) -> None:
        pass

    async def _publish_message_to_failed_topic(self, kafka_message, error: Exception) -> None:
        ...
