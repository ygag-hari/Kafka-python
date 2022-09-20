import json
from confluent_kafka import Consumer


c = Consumer({
    'bootstrap.servers':'kafka:9092',
    'group.id':'python-consumer',
    'auto.offset.reset':'earliest'
})

print('Available topics to consume: ', c.list_topics().topics)
c.subscribe(['ygag-orders'])

while True:
    msg=c.poll(1.0) #timeout
    if msg is None:
        continue
    if msg.error():
        print('Error: {}'.format(msg.error()))
        continue
    try:
        data=msg.value().decode('utf-8')
        data = json.loads(data)
        print("consuming....")
        print(data)
    except Exception as e:
        print(e)
