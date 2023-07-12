import json
from kafka import KafkaConsumer
import config
topic_name = "nhac_vang"
c = KafkaConsumer(
    topic_name,
    bootstrap_servers = [config.kafka_ip],
    auto_offset_reset = 'latest',
    enable_auto_commit = True
)



for message in c:
    print("Da nhan ", message.value)