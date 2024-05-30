import json
import time

from kafka import KafkaProducer
from kafka import TopicPartition
from kafka import KafkaConsumer

topic = "compra_detalles"
topicNotif = "compra_confirmada"

consumer = KafkaConsumer(topic, bootstrap_servers="localhost:29092", auto_offset_reset='earliest', enable_auto_commit=True, auto_commit_interval_ms=1000, group_id='newGroup')
producer = KafkaProducer(bootstrap_servers="localhost:29092")

while True:
    print("Buscando transacciones... \n")
    for message in consumer:
        consumed_order = json.loads(message.value.decode('utf-8'))
        orderid = consumed_order["orderid"]
        gameid = consumed_order["gameid"]
        price = consumed_order["price"]
        name = consumed_order["name"]
        usermail = consumed_order["usermail"]
        data = {
            "orderid": orderid,
            "gameid": gameid,
            "name": name,
            "price": price,
            "usermail": usermail,
            "estado": "recibido",
        }
        producer.send(topicNotif, json.dumps(data).encode("utf-8"))
        time.sleep(5)
        data["estado"] = "preparando"
        producer.send(topicNotif, json.dumps(data).encode("utf-8"))
        time.sleep(5)
        data["estado"] = "entregando"
        producer.send(topicNotif, json.dumps(data).encode("utf-8"))
        time.sleep(5)
        data["estado"] = "finalizado"
        producer.send(topicNotif, json.dumps(data).encode("utf-8"))
        time.sleep(5)
