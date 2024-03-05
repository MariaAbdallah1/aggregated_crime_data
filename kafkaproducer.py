# scripts/kafka_producer.py
from kafka import KafkaProducer
import json
import os
import pandas as pd
def produce_to_kafka():
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Read IMDb dataset
    imdb_data = pd.read_csv('01_District_wise_crimes_committed_IPC_2001_2012.csv')

    # Convert each row to JSON and send it to the Kafka topic
    for _, row in imdb_data.iterrows():
        message = json.dumps(row.to_dict())
        producer.send('test', value=message.encode('utf-8'))

    producer.close()

if __name__ == "__main__":
    produce_to_kafka()
