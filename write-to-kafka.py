from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import SparkSession



# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("id", IntegerType()).add("state", StringType()).add("Murder", IntegerType()).add("AttemptToMurder", IntegerType()).add("KidnappingAndAbduction", IntegerType()).add("KidnappingAndAbductionOfWomenAndGirls", IntegerType())

# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("path", "C:/Users/DELL/Desktop/maria_project/data") \
    .load() \

# Select specific columns from "data"
#df = streaming_df.select("name", "age")

#df = streaming_df.select(col("name").alias("key"), to_json(col("age")).alias("value"))
df = streaming_df.select(col("state"), to_csv(struct("*")).alias("value"))
# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
