4. **`kafka_test.py`** (for testing the Kafka setup):

# kafka_test.py
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import json

# Kafka broker configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'myTopic'

# Create a Kafka consumer
consumer = KafkaConsumer(topic_name, group_id='myGroup',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Verify Kafka producer-consumer flow
def test_kafka_producer_consumer():
    # Produce random events to Kafka topic
    # (This assumes kafka_producer.py has already run)
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("KafkaTest").getOrCreate()

    # Read data from Iceberg table
    df_from_iceberg = spark.read.format("iceberg").load("my_iceberg_catalog.db.empdata")

    # Consume messages from Kafka topic
    for message in consumer:
        event = message.value

        # Validate if the consumed event is present in the Iceberg table
        matching_records = df_from_iceberg.filter(
            (df_from_iceberg.Name == event["EventName"]) &
            (df_from_iceberg.Job == event["EventType"]) &
            (df_from_iceberg.Number == event["EventValue"]) &
            (df_from_iceberg.source == event["EventPageSource"]) &
	    (df_from_iceberg.URL  == event["EventPageURL"]) &
	    (df_from_iceberg.CID == event["EventComponentID"]) &
	    (df_from_iceberg.ID == event["EventUserID"]) 
        )

        assert matching_records.count() > 0, f"Event not found in Iceberg table: {event}"

        print(f"Event validated and found in Iceberg table: {event}")

    # Stop Spark session
    spark.stop()

# Run the test
test_kafka_producer_consumer()
