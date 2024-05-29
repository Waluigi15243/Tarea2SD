import json
from flask import Flask
from kafka import KafkaConsumer
from threading import Thread

app = Flask(__name__)
topicNotif = "compra_confirmada"
diccionarios = []

consumer = KafkaConsumer(topicNotif, bootstrap_servers="localhost:29092", auto_offset_reset='earliest', enable_auto_commit=True, auto_commit_interval_ms=1000, group_id='newGroup')
def mail():
  while True:
    for message in consumer:
      global diccionarios
      order = json.loads(message.value.decode('utf-8'))
      if order["estado"] == "recibido":
        diccionarios.append(order)
    
@app.route('/')
def index():
  global diccionarios
  i = int(request.args.get('value'))
  peticion = diccionarios[i-1]['estado']
  return f"El estado actual de la transaccion es el siguiente: {peticion}"

if __name__ == '__main__':
  threads = Thread(target=mail)
  threads.start()
  app.run(debug=True, port=3000)
