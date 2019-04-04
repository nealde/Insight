import kafka
# connect to Kafka cluster
cluster = kafka.KafkaClient("10.0.0.11:9092")
topic="cloud"
consumer_group = 'default_group'
consumer = kafka.SimpleConsumer(cluster, consumer_group, topic)

# consume all messages
for raw in consumer:
    msg = raw.message.value
    print(msg)
