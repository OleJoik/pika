
import pika
import json

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

queue = channel.queue_declare('order_notify')
queue_name = queue.method.queue

channel.queue_bind(
    exchange='order',
    queue=queue_name,
    routing_key='order.notify'
)

def callback(
        ch: BlockingChannel, 
        method: Basic.Deliver, 
        properties: BasicProperties,
        body: bytes
    ):
    payload = json.loads(body)

    print(' [x] Notifying {}'.format(payload['user_email']))
    print(' [x] Done')

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(on_message_callback=callback, queue=queue_name)

print(' [*] Waiting for notify messages. To exit pret CTRL + C')

channel.start_consuming()