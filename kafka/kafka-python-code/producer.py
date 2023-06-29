import time 
import json
from ticketmaster_api_fetch import fetch_ticketmaster_data
from kafka import KafkaProducer


## SHELL SCRIPT TO RUN FROM CLI 
import sys
# Read CLI arguments
if len(sys.argv) > 2:
    city = sys.argv[1]
    state = sys.argv[2]
else:
    city = 'Austin'
    state = 'TX'

# Use the city and state to fetch_ticketmaster_data function
df = fetch_ticketmaster_data(city, state, 'US')


## MANUAL INPUT 
# df = fetch_ticketmaster_data('New York','NY','US')

#Optional - save the data to a csv file
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
    time.sleep(0.25)

# Close the Kafka producer
producer.close()
