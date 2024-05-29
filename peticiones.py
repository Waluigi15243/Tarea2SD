import json
import time
from random import randrange
from flask import Flask, request
from kafka import KafkaProducer
import csv

app = Flask(__name__)
topic = "compra_detalles"
transaction = 0

producer = KafkaProducer(bootstrap_servers="localhost:29092")
@app.route('/', methods=['POST'])
def sendOrder():
    with open("steamgames.csv") as archivo:
        i = int(request.form['value'])
        data = csv.reader(archivo)
        for _ in range(i):
            next(data)
        row = next(data)
        number = randrange(2000)
        transaction += 1
        order = {
            "orderid": transaction,
            "gameid": row[0],
            "name": row[1],
            "price": row[2],
            "usermail": f"{number}@correo.com",
        }
        print(order + "\n")
        producer.send(topic, json.dumps(order).encode("utf-8"))
        print("Transaccion enviada!")
        return "Transaccion enviada!"

if __name__ == '__main__':
    app.run(debug=True, port=3000)
