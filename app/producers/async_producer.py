import asyncio
from typing import Any, Self

from aiokafka import AIOKafkaProducer

from app.configuretion import get_producer_kafka_settings


class AsyncKafkaProducer:

    def __init__(self, topic: str, aio_producer: AIOKafkaProducer) -> None:
        self._topic = topic
        self._producer = aio_producer
        print("Producer created")

    @classmethod
    async def create_instance(cls, topic: str) -> Self:
        prod = AIOKafkaProducer(
            bootstrap_servers=get_producer_kafka_settings().bootstrap_servers,
            key_serializer=get_producer_kafka_settings().get_key_serializer(),
            value_serializer=get_producer_kafka_settings().get_value_serializer(),
            max_batch_size=get_producer_kafka_settings().max_batch_size,
        )
        await prod.start()

        return cls(
            topic=topic,
            aio_producer=prod
        )

    async def stop_producer(self) -> None:
        if self._producer:
            await self._producer.stop()

        self._producer = None

    async def send(
            self,
            value: Any,
            key: str = None,
            partition: int | None = None,
            headers: dict[str, Any] = None,
    ) -> None:
        await self._producer.send_and_wait(
            topic=self._topic,
            value=value,
            key=key,
            partition=partition,
            headers=headers,
        )

    async def send_batch(
        self,
        value: Any,
        key: str = None,
        partition: int = 0,
        headers: dict[str, Any] = None,
    ) -> None:
        await self._producer.send(
            topic=self._topic,
            value=value,
            key=key,
            partition=partition,
            headers=headers,
        )


async def send_test():
    producer = await AsyncKafkaProducer.create_instance(
        topic='test'
    )

    for i in range(33):
        await asyncio.sleep(1)
        await producer.send_batch(
            value=f"test-{i}",
            key=f'{i}-key',
            partition=0,
            headers=[(f"{i}", b"21w123")]
        )
    await producer.stop_producer()


if __name__ == '__main__':
    asyncio.run(send_test())
