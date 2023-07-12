import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import config
topic_name = "camera_out"
c = KafkaConsumer(
    topic_name,
    bootstrap_servers = [config.kafka_ip],
    auto_offset_reset = 'latest',
    enable_auto_commit = True,
    fetch_max_bytes = 9000000,
    fetch_max_wait_ms = 10000,
)

for message in c:
    stream = message.value
    # Chuyen thanh hinh anh
    stream = np.frombuffer(stream, dtype=np.uint8)
    image = cv2.imdecode(stream, cv2.IMREAD_COLOR)
    print("Doc hinh anh")
    cv2.imshow("ket qua", image)
    cv2.waitKey(1)