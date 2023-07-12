from kafka import  KafkaConsumer
import json
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')

db = client['searches']
collection = db['search']

ORDER_KAFKA_CONFIRMED_TOPIC="search_details"

consumer=KafkaConsumer(
    ORDER_KAFKA_CONFIRMED_TOPIC,
    bootstrap_servers="localhost:9092"
)


print("Analytic backend is listening")

while True:
    for message in consumer:
        consumed_message=json.loads(message.value.decode())
        collection.insert_one(consumed_message)
        time=consumed_message["time"]
        city = consumed_message["city"]
        word = consumed_message["search"]

        print(f"Searched: {word}")