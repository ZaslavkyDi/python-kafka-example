import asyncio
from typing import Any

from aiokafka import ConsumerRecord

from app.consumers.basic_async_consumer import BaseAsyncKafkaConsumer


class ExampleAsyncKafkaConsumer(BaseAsyncKafkaConsumer):

    def __init__(self) -> None:
        super().__init__(
            topics=["topic-example", "test"],
            group_id="test-example-group",
        )

    async def _process_message(self, kafka_message: ConsumerRecord) -> None:
        print(kafka_message.value)


async def main():
    p = ExampleAsyncKafkaConsumer()
    try:
        await p.start_consumer()
    finally:
        await p.stop_consumer()


if __name__ == '__main__':
    asyncio.run(main())
