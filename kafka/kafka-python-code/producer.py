import time 
import json 
from datetime import datetime
from ticketmaster_api_fetch import fetch_ticketmaster_data
from kafka import KafkaProducer
import pandas as pd



## INPUT 
df = fetch_ticketmaster_data('New York','NY','US')
# df.head()
# df.to_csv('/Users/nicburkett/Downloads/houston_data.csv')

## SET UP THE KAFKA PRODUCER 

topic_name = 'ticketmaster'
# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

## Clean out the producer 
producer.flush() 

# Convert each row to JSON and send as message to Kafka
for _, row in df.iterrows():
    # Convert the row to a dictionary with column names as keys
    data = row.to_dict()

    # Include the column names in the dictionary
    message = {col: data[col] for col in df.columns}

    # Print the JSON message
    print(message)
    
    # Send the JSON message to the Kafka topic
    producer.send(topic_name, message)
    time.sleep(5)

# Close the Kafka producer
producer.close()
