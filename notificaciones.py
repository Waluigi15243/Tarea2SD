import json
import smtplib
from flask import Flask, request
from kafka import KafkaConsumer
from threading import Thread, Lock
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)
topicNotif = "compra_confirmada"
diccionarios = []
diccionariosLock = Lock()

consumer = KafkaConsumer(topicNotif, bootstrap_servers="localhost:29092", auto_offset_reset='earliest', enable_auto_commit=True, auto_commit_interval_ms=1000, group_id='newGroup')

def createMail(subject, body, fromAddress, toAddress):
  message = MIMEMultipart()
  message['From'] = fromAddress
  message['To'] = toAddress
  message['Subject'] = subject
  message.attach(MIMEText(body, 'plain'))
  return message

def sendMail(emailMsg, fromAddress):
  server = smtplib.SMTP("smtp.gmail.com", 587)
  server.starttls()
  server.login(fromAddress, "hjiy nkhv nqcx rixx")
  server.send_message(emailMsg)
  server.quit()

def mail():
  while True:
    for message in consumer:
      global diccionarios
      order = json.loads(message.value.decode('utf-8'))
      name = order["name"]
      usermail = order["usermail"]
      price = order["price"]
      fromAddress = "tempamogus.homework@gmail.com"
      toAddress = order["usermail"]
      subject = "Estado de la Transaccion"
      with diccionariosLock:
        if order["estado"] == "recibido":
          diccionarios.append(order)
          body = f"Estimado Usuario: \n Le enviamos este correo para informarle a usted que se ha recibido una transaccion en la cual se senala que usted ha comprado {name} a un costo de ${price}. \n Para mas informacion, actualice el historial de su correo para verificar el estado de su transaccion"
          emailMsg = createMail(subject, body, fromAddress, toAddress)
          sendMail(emailMsg, fromAddress)
        elif order["estado"] == "preparando":
          diccionarios[-1]["estado"] = order["estado"]
          body = f"Estimado Usuario: \n Le enviamos este correo para informarle a usted que su transaccion se encuentra en proceso de preparacion. Recuerde que su compra corresponde a {name}, y su costo fue de ${price}. \n Para mas informacion, actualice el historial de su correo para verificar el estado de su transaccion"
          emailMsg = createMail(subject, body, fromAddress, toAddress)
          sendMail(emailMsg, fromAddress)
        elif order["estado"] == "entregando":
          diccionarios[-1]["estado"] = order["estado"]
          body = f"Estimado Usuario: \n Le enviamos este correo para informarle a usted que su transaccion acaba de prepararse y se encuentra en el proceso de entregado. Recuerde que su compra corresponde a {name}, y su costo fue de ${price}. \n Para mas informacion, actualice el historial de su correo para verificar el estado de su transaccion"
          emailMsg = createMail(subject, body, fromAddress, toAddress)
          sendMail(emailMsg, fromAddress)
        elif order["estado"] == "finalizado":
          diccionarios[-1]["estado"] = order["estado"]
          body = f"Estimado Usuario: \n Le enviamos este correo para informarle a usted que su transaccion ha finalizado, y ahora usted tiene el videojuego {name} en su posicion. \n Ojala el servicio haya sido de su agrado, no dude en realizar otro pedido!"
          emailMsg = createMail(subject, body, fromAddress, toAddress)
          sendMail(emailMsg, fromAddress)


@app.route('/')
def index():
  global diccionarios
  i = int(request.args.get('value'))
  with diccionariosLock:
    for transaction in diccionarios:
      if transaction["orderid"] == i:
        return transaction
  return "ERROR: No existe ninguna transaccion con ese numero ID"

if __name__ == '__main__':
  threads = Thread(target=mail)
  threads.start()
  app.run(debug=True, port=5000)
