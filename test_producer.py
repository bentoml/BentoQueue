import asyncio
import os

import aioconsole

MQ_URL = os.getenv("RABBITMQ_URL", "amqp://admin:5882352941@184.105.5.135")
MQ_NAME = os.getenv("REBBITMQ_QUEUE_NAME", "test_queue")


async def main():
    loop = asyncio.get_event_loop()
    import aio_pika

    connection = await aio_pika.connect_robust(MQ_URL, loop=loop)
    print("Connected to RabbitMQ")

    async with connection:
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        while True:
            input_str = await aioconsole.ainput("Enter a message: ")
            times = int(await aioconsole.ainput("Enter number of times to send: "))

            for _ in range(times):
                await channel.default_exchange.publish(
                    aio_pika.Message(body=input_str.encode()),
                    routing_key=MQ_NAME,
                )


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
