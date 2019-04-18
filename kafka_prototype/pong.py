from kafka import KafkaProducer, KafkaConsumer
consumer = KafkaConsumer('foobar', bootstrap_servers='10.0.0.8:9092')
producer = KafkaProducer(bootstrap_servers='10.0.0.8:9092')
for msg in consumer:
    print('100 messages received')
    for _ in range(100):
        producer.send('cloud', b'some_message_bytes')

