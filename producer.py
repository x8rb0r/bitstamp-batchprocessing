# Databricks notebook source
!pip install confluent_kafka
!pip install kafka-python

# COMMAND ----------

import json
import requests
import pandas as pd

from time import sleep
from json import dumps
from kafka import KafkaProducer

import sys
import os
import socket
import time

from confluent_kafka import Producer,SerializingProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# COMMAND ----------

topic="yxpn0jur-default"
   
conf = {
        'bootstrap.servers': "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094",
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'key.serializer':StringSerializer('utf_8'),
        'value.serializer': StringSerializer('utf_8'),
        'sasl.username': "[secret]",
        'sasl.password': "[secret]"
}
p = SerializingProducer(conf)

# COMMAND ----------

while True:
    try:
        response = requests.get('https://www.bitstamp.net/api/v2/transactions/btcusd/')
        if response.status_code == 200:
            for transaction in pd.DataFrame.from_dict(response.json()).loc[:,['price','amount','type']].values.tolist():
              p.produce(topic,key = "key", value = transaction[0] + "," + transaction[1] + "," + transaction[2])
    except:
      pass
    print("produce... (every 10 seconds)")
    sleep(10)