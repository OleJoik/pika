
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage


async def process_message(
    message: AbstractIncomingMessage
) -> None:
    async with message.process():
        print(message.body)
        await asyncio.sleep(1)

async def main() -> None:
    connection = await aio_pika.connect_robust(host='localhost')
    queue_name = "test_queue"

    # Creating channel
    channel = await connection.channel()
    
    # Maximum message count which will be processing at the same time. 
    await channel.set_qos(prefetch_count=100)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, auto_delete=True)

    await queue.consume(process_message)

    try:
        await asyncio.Future()
    finally:
        await connection.close()

if __name__ == "__main__":
    asyncio.run(main())