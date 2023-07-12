import json

import config
from kafka import KafkaProducer

topic_name = "nhac_vang"

p = KafkaProducer(
    bootstrap_servers = [config.kafka_ip]
)

json_mess = json.dumps({"abc":"bac"})

p.send( topic_name, json_mess.encode("utf-8"))
p.flush()

print("Da gui")
