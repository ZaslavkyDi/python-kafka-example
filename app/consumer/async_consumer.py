import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from app.configuretion import get_consumer_kafka_settings

logger = logging.getLogger(__name__)


class AsyncKafkaConsumer:

    def __init__(self, topics: list[str], group_id: str) -> None:
        self._topics = topics
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    def build_aio_consumer(self) -> AIOKafkaConsumer:
        if self._consumer:
            return self._consumer

        self._consumer = AIOKafkaConsumer(
            "test",
            group_id=self._group_id,
            enable_auto_commit=False,
            bootstrap_servers=get_consumer_kafka_settings().bootstrap_servers,
            auto_offset_reset=get_consumer_kafka_settings().auto_offset_reset,
            value_deserializer=lambda x: x.decode('utf-8'),
        )
        return self._consumer

    async def stat_consume(self) -> None:
        consumer = self.build_aio_consumer()
        print("Consumer created")

        await consumer.start()

        print("start listen for messages")
        async for message in consumer:
            try:
                await self.process_message(kafka_message=message)
                await consumer.commit()
            except Exception as e:
                logger.error(str(e))
                await self._publish_message_to_failed_topic(
                    kafka_message=message,
                    error=e,
                )

    async def process_message(self, kafka_message) -> None:
        print(kafka_message)

    async def stop_consume(self) -> None:
        if self._consumer:
            await self._consumer.stop()

        self._consumer = None

    async def _publish_message_to_failed_topic(self, kafka_message, error: Exception) -> None:
        ...


if __name__ == "__main__":
     c = AsyncKafkaConsumer(topics=['test'], group_id='test_group')

     asyncio.run(c.stat_consume())
     print()