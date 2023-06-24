import json 
from kafka import KafkaConsumer
import kafka



topic_name = 'twitter'
if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        print(json.loads(message.value))