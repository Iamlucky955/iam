1.**`kafka_producer.py`** (for producing random data to Kafka topic):

```python
# kafka_producer.py
from kafka import KafkaProducer
import json
import random

# Kafka broker configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'myTopic'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

# Generate random data (replace with your own logic)
def generate_random_event():
    return {
        "EventName": "Event_" + str(random.randint(1, 100)),
        "EventType": "Type_" + str(random.choice(["A", "B", "C"])),
        "EventValue": random.uniform(0.1, 10.0),
        "EventPageSource": "Page_" + str(random.randint(1, 10)),
        "EventPageURL": "https://example.com/page_" + str(random.randint(1, 100)),
        "ComponentID": random.randint(1000, 9999),
        "UserID": random.randint(10000, 99999)
    }

# Produce random events to Kafka topic
for _ in range(100):
    event_data = generate_random_event()
    producer.send(topic_name, value=event_data)

print("Random events produced successfully!")
