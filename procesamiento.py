import json

from kafka import KafkaProducer
from kafka import KafkaConsumer

topic = "detalle_compra"
topicOrderArrived = "compra_recibida"
topicOrderProcessing = "procesando_compra"
topicOrderDelivery = "entregando_compra"
topicOrderConfirmed = "compra_confirmada"