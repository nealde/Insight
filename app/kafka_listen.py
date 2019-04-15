import kafka
# connect to Kafka cluster
cluster = kafka.KafkaClient("10.0.0.11:9092")
topic="cloud"
consumer_group = 'default_group'


# consume all messages
global kafka_count
kafka_count = 0

def listen():
    consumer = kafka.SimpleConsumer(cluster, consumer_group, topic)
    for raw in consumer:
        kafka_count+=1
    return kafka_count
