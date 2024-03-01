2. **`kafka_consumer.py`** (for consuming data from Kafka topic):

# kafka_consumer.py

from kafka import KafkaConsumer
import json

# Kafka broker configuration

bootstrap_servers = ['localhost:9092']
topic_name = 'myTopic'

# Create a Kafka consumer

consumer = KafkaConsumer(topic_name, group_id='myGroup',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Consume messages from Kafka topic

for message in consumer:
    event = message.value
    print(f"Received event: {event}")
