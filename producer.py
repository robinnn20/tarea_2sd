from confluent_kafka import Producer
import time
from uuid import uuid4

producer_conf = {
    'bootstrap.servers': 'localhost:9093',
    'client.id': 'productor1'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al entregar mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

for i in range(100):
    message = f"Holaaaa {i}"
    key = str(uuid4())
    print(f"Key: {key} Produciendo mensaje: {message}")
    producer.produce('orders', key=key, value=message, callback=delivery_report)
    time.sleep(1)
    producer.poll(0)

producer.flush()
