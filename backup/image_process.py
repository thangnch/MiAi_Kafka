import cv2
from PIL import Image
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
in_topic_name = "miai_input"
out_topic_name = "miai_output"
import config
consumer = KafkaConsumer(
    in_topic_name,
    bootstrap_servers=[config.ip],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=None,
    fetch_max_bytes=52428800,
    fetch_max_wait_ms = 10000
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[config.ip],
    max_request_size=9048576,
    compression_type = 'gzip'
)

for message in consumer:
    # Get imageb64
    # try:
    stream = message.value
    image = cv2.imdecode(np.frombuffer(stream, dtype=np.uint8), cv2.IMREAD_COLOR)
    # image = Image.open(stream).convert('RGBA')
    print(image.shape)

    cv2.putText(image, "PROCESSED", (100,100), cv2.FONT_HERSHEY_SIMPLEX, 2, (255,255,255),2)
    # cv2.imwrite("stream.png", image)
    # stream.close()
    print("Send to kafka")
    ret, buffer = cv2.imencode('.jpg', image)
    producer.send( out_topic_name , buffer.tobytes())#.encode("utf-8"))
    producer.flush()

