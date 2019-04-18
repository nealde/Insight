from kafka import KafkaConsumer
consumer = KafkaConsumer('cloud', bootstrap_servers='10.0.0.11:9092')
for msg in consumer:
    print (msg)
