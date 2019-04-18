#import kafka
from kafka import KafkaProducer
import logging
#logging.basicConfig(level=logging.DEBUG)
#cluster= kafka.KafkaClient("10.0.0.8:9092")
producer = KafkaProducer(bootstrap_servers='10.0.0.8:9092')
for _ in range(100):
    producer.send('foobar', b'some_message_bytes')
