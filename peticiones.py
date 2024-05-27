import json
import time

from kafka import KafkaProducer
import csv

topic = "detalle_compra"

producer = KafkaProducer(bootstrap_servers="localhost:9092")

with open("steamgames.csv") as archivo:
    data = csv.reader(archivo)
    for _ in range(4):
        next(data)
    row = next(data)
    order = {
        "id": row[0],
        "name": row[1],
        "price": row[2],
    }
    print(order)
    producer.send(topic, json.dumps(order).encode("utf-8"))
    time.sleep(10)
