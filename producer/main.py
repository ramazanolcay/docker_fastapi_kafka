from fastapi import FastAPI
from datetime import datetime
import numpy as np
import json 
from kafka import KafkaProducer
import time

producer=KafkaProducer(bootstrap_servers="localhost:9092")

app = FastAPI()

print("Kafka is listening...")

@app.get("/search/{word}")
def search_word(word: str):
    city_list = ["Aydın","İzmir","Muğla", "Ankara","Antalya","Amsterdam","Prague","Denizli","İstanbul"]
    city = np.random.choice(city_list)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data={
        "time":current_time,
        "city":city,
        "search": word
    }

    producer.send(
        "search_details",
        json.dumps(data).encode("utf-8")
    )
    time.sleep(1)
    print("Done sending...")
    return {"time": current_time, "word": word,"city": city}

