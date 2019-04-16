import kafka
import time
# connect to Kafka cluster
cluster = kafka.KafkaClient("10.0.0.11:9092")
topic="cloud"
consumer_group = 'default_group'


# consume all messages
#kafka_count = 0
#global kafka_count

def listen(kafka_count):
    consumer = kafka.SimpleConsumer(cluster,group=None, topic=topic, partitions=[0, 1])
    print(consumer)
    for raw in consumer:
        print(len(raw))
        kafka_count+=1
    return kafka_count
n = 0
while True:
    n = listen(n)
    time.sleep(1)
    print(n)
