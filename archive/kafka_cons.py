import kafka


consumer = kafka.KafkaConsumer("job_topic", bootstrap_servers="10.0.9.23:9092")

for message in consumer:
    print(message.value)
    