# Importing the KafkaConsumer class from the kafka module
import kafka

# Creating an instance of KafkaConsumer
# 'job_topic' is the topic name to subscribe to
# 'bootstrap_servers' specifies the Kafka server address
consumer = kafka.KafkaConsumer("job_topic", bootstrap_servers="10.0.9.23:9092")

# Iterating over the messages in the topic
for message in consumer:
    # Printing the value of each message consumed from the topic
    print(message.value)
