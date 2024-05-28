import json
import time
from random import randrange

from kafka import KafkaProducer
import csv

topic = "compra_detalles"

producer = KafkaProducer(bootstrap_servers="localhost:29092")

with open("steamgames.csv") as archivo:
    data = csv.reader(archivo)
    for _ in range(4):
        next(data)
    row = next(data)
    number = randrange(2000)
    order = {
        "orderid": number,
        "gameid": row[0],
        "name": row[1],
        "price": row[2],
        "usermail": f"{number}@correo.com",
    }
    print(order)
    producer.send(topic, json.dumps(order).encode("utf-8"))
    print("Transaccion enviada!")
