3. **`kafka_pyspark.py`** (for generating random data using PySpark):


# kafka_pyspark.py
from pyspark.sql import SparkSession
from faker import Faker

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaPySpark").getOrCreate()

# Generate random data using Faker library
fake = Faker()
num_records = 1000

data = [(fake.EventName(), fake.EventType(), fake.EventValue(), fake.EventPageSource(), fake.EventPageURL, fake.ComponentID(), fake.USERID(min=18, max=80)) for _ in range(num_records)]
columns = ["Name", "Job", "Number","Source","URL","CID","ID"]

df = spark.createDataFrame(data, columns)

# Write data to Iceberg table
df.write.format("iceberg").mode("append").save("my_iceberg_catalog.db.empdata")

print(f"{num_records} random records added to the Iceberg table.")

# Stop Spark session
spark.stop()
