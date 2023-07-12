import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'miai3',
    bootstrap_servers=['192.168.68.125:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=None


)

for message in consumer:
    print(message.value)
