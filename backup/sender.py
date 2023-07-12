import time
import json
import random
from datetime import datetime

from kafka import KafkaProducer



# Kafka Producer
ip = "192.168.137.171:9092"
producer = KafkaProducer(
    bootstrap_servers=[ip]
)

if __name__ == '__main__':
    # Infinite loop - runs until you kill the program
    # while True:
    # Generate a message
    dummy_message = json.dumps( {"message": 1})

    # Send it to our 'messages' topic
    print(f'Producing message @ {datetime.now()} | Message = {str(dummy_message)}')
    f = producer.send('miai_input', dummy_message.encode("utf-8"))
    producer.flush()

    # Sleep for a random number of seconds
    time_to_sleep =1
    time.sleep(time_to_sleep)