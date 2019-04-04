import kafka

# connect to kafka cluster
cluster= kafka.KafkaClient("10.0.0.11:9092")
prod = kafka.SimpleProducer(cluster, async=False)

# produce some messages
topic="cloud"
msg_list = ["first message", "second message"]
prod.send_messages(topic, *msg_list)
