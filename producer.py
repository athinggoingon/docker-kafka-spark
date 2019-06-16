""" Generate test data to send to Kafka """

import random
from time import sleep
from json import dumps

from kafka import KafkaProducer, KafkaClient

TEST_DATA = [
    {'id': 1, 'foo': 'bar'},
    {'id': 2, 'foo': 'baz'},
    {'id': 3, 'foo': 'bnat'}
]

KAFKA = KafkaClient('localhost:9092')

PRODUCER = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='test-producer'
)

TOPIC = 'test-topic'

while True:
    index = random.randint(0, len(TEST_DATA) - 1)
    record = TEST_DATA[index]

    PRODUCER.send(TOPIC, dumps(record).encode('utf-8'))

    print('pushed: {}'.format(index))

    # Send records at random intervals
    sleep(random.uniform(0.01, 5))
