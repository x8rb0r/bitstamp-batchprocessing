# Databricks notebook source
!pip install confluent_kafka
!pip install kafka-python

# COMMAND ----------

import sys
import os

from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

# COMMAND ----------

topics=["yxpn0jur-default"]

conf = {
  'bootstrap.servers': "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094",
  'group.id': "[secret]",
  'session.timeout.ms': 6000,
  'default.topic.config': {'auto.offset.reset': 'largest'},
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'SCRAM-SHA-256',
  'key.deserializer':StringDeserializer('utf_8'),
  'value.deserializer': StringDeserializer('utf_8'),
  'sasl.username': "[secret]",
  'sasl.password': "[secret]"
}

c = DeserializingConsumer(conf)
c.subscribe(topics)

# COMMAND ----------

import socket
import time

# COMMAND ----------

host = 'localhost'
# a port változik az előző feladathoz képest
port = 12346

i = 0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind((host, port))
s.listen(1)

try:
    while True:
        print("waiting...")
        conn, addr = s.accept()
        print("it's arrived...")
        try:
            while True:
                try:
                    msg = c.poll(1.0)
                    if msg is None:
                        print("nothing came...")
                        continue

                    user = msg.value()
                    if user is not None:
                        print(user)
                        conn.send(bytes("{}\n".format(user), "utf-8"))
                except KeyboardInterrupt:
                        break
            conn.close()
        except socket.error: pass
finally:
    s.close()