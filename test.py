from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql

#conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
#cursor = conn.cursor()
def insert_into_phpmyadmin(batch_df, batch_id):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "Crime_bigData"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Iterate over each row in the DataFrame and execute the SQL query
    for row in batch_df.collect():
        # Extract the required columns from the row
        state = row.state
        sum_murder = row.Murder
        sum_attempt = row.AttemptToMurder
        sum_kidnapping = row.KidnappingAndAbduction
        sum_kidnapping_women = row.KidnappingAndAbductionOfWomenAndGirls

        # Prepare the SQL query to insert data into the table
        sql_query = f"INSERT INTO aggregated_crime_data (state, sum_Murder, sum_AttemptToMurder, sum_KidnappingAndAbduction, sum_KidnappingAndAbductionOfWomenAndGirls) " \
                    f"VALUES ('{state}', {sum_murder}, {sum_attempt}, {sum_kidnapping}, {sum_kidnapping_women})"

        # Execute the SQL query
        cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("state", StringType()).add("Murder", IntegerType()).add("AttemptToMurder", IntegerType()).add("KidnappingAndAbduction", IntegerType()).add("KidnappingAndAbductionOfWomenAndGirls", IntegerType())


# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \


# Assuming the value column in Kafka contains CSV data
#df = df.selectExpr("CAST(value AS STRING)")

# Split the CSV data and cast to the specified schema
df = df.select("data.state", "data.Murder")
'''df = df.select(
    split(col("value"), ",").alias("data")
).select(
    col("data").getItem(0).alias("state"),
    col("data").getItem(1).cast(IntegerType()).alias("Murder"),
    col("data").getItem(2).cast(IntegerType()).alias("AttemptToMurder"),
    col("data").getItem(3).cast(IntegerType()).alias("KidnappingAndAbduction"),
    col("data").getItem(4).cast(IntegerType()).alias("KidnappingAndAbductionOfWomenAndGirls")
)'''
# Read CSV directly with the specified schema
#df = df.select(from_csv(col("value"), schema=schema).alias("data"))

# Select specific columns from "data"
# Aggregate the data by state and calculate the sum of each column
agg_df = df.groupBy("state").agg(
    sum("Murder").alias("sum_Murder"),
)

# Select specific columns from the aggregated DataFrame
'''
agg_df = agg_df.select(
    "state",
    "sum_Murder",
    "sum_AttemptToMurder",
    "sum_KidnappingAndAbduction",
    "sum_KidnappingAndAbductionOfWomenAndGirls"
)
'''
# Convert the value column to string and display the result
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for the query to finish
query.awaitTermination()