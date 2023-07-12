import cv2
import time
import json
import base64
import config
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka Producer
topic_name = "miai_input"

producer = KafkaProducer(
    bootstrap_servers=[config.ip],
    max_request_size=9048576,
    retries = 10,

)

cam = cv2.VideoCapture(1)
last_send = datetime.now() + timedelta(seconds=-10)

while True:
    ret, frame = cam.read()
    if ret:
        frame = cv2.resize(frame, dsize=None, fx=0.5, fy=0.5)
        # cv2.imshow ("Cam", frame)
        # Push to Kafka after 10 second
        current = datetime.now()
        if current  > last_send + timedelta(seconds=10):
            # msg = base64.b64encode(frame)

            # msg = json.dumps({
            #     "frame": imageb64,
            #     "send_time": str(current)
            # })
            # print(len(msg))

            print(f'Producing message @ {current}')
            ret, buffer = cv2.imencode('.jpg', frame)
            f = producer.send( topic_name ,  buffer.tobytes())#.encode("utf-8"))
            producer.flush()
            last_send = datetime.now()

    if cv2.waitKey(1) == ord('q'):
        break

cv2.destroyAllWindows()
cam.release()