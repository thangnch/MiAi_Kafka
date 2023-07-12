import cv2
from PIL import Image
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
out_topic_name = "miai_output"
import config
consumer = KafkaConsumer(
    out_topic_name,
    bootstrap_servers=[config.ip],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=None,
    fetch_max_bytes=52428800,
    fetch_max_wait_ms = 10000
)

for message in consumer:
    # Get imageb64
    # try:
    stream = message.value
    image = cv2.imdecode(np.frombuffer(stream, dtype=np.uint8), cv2.IMREAD_COLOR)
    cv2.imshow("stream.png", image)
    print("Show")
    cv2.waitKey(1)

