import asyncio

from service import MQ_URL, MQ_NAME


async def main():
    loop = asyncio.get_event_loop()
    import aio_pika

    connection = await aio_pika.connect_robust(MQ_URL, loop=loop)
    print("Connected to RabbitMQ")

    async with connection:
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        while True:
            input_str = input("Enter message to send: ")
            times = int(input("Enter number of times to send: "))
            interval = float(input("Enter interval between messages: "))

            for _ in range(times):
                await channel.default_exchange.publish(
                    aio_pika.Message(body=input_str.encode()),
                    routing_key=MQ_NAME,
                )
                print(f"Sent message: {input_str}")
                await asyncio.sleep(interval)


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
