import json

from kafka import KafkaProducer
from kafka import KafkaConsumer

topic = "compra_detalles"
topicNotif = "compra_confirmada"

consumer = KafkaConsumer(topic, bootstrap_servers="localhost:29092")
producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Buscando transacciones...")
while True:
    for message in consumer:
        print("Recibiendo transaccion...")
        consumed_order = json.loads(message.value.decode())
        print(consumed_order)
        #orderid = consumed_order["orderid"]
        #gameid = consumed_order["gameid"]
        #price = consumed_order["price"]
        #name = consumed_order["name"]
        #usermail = consumed_order["usermail"]
        #data = {
        #    "orderid": orderid,
        #    "gameid": gameid,
        #    "nombre": name,
        #    "price": price,
        #    "usermail": f"{userid}@correo.com",
        #    "estado": "recibido",
        #}
        #print("Transaccion recibida!")
        #producer.send(topicNotif, json.dumps(data).encode("utf-8"))
