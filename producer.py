import time
import json
from confluent_kafka import Producer
import datetime


# initializing the Kafka producer
p = Producer({'bootstrap.servers': 'kafka:29092'})

data = {
    "date_added": "2022-02-23",
}

for _ in range(100):
    print("Producing epoch {}".format(_))
    m=json.dumps(data)
    p.poll(1)
    p.produce('ygag-orders', m.encode('utf-8'))
    p.flush()
    time.sleep(3)
