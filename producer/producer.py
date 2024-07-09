import json 
import time
import pandas as pd
from kafka import KafkaProducer

from data_extractor import get_article

conf = { "bootstrap_servers":'localhost:9092',
        "value_serializer":lambda v: json.dumps(v).encode('utf-8')
        }

producer = KafkaProducer(**conf)

def publish_data():
    while True:
        data = json.loads(get_article())
        if not data:
            break
        producer.send('Topic1', data)
        time.sleep(120)

if __name__ == '__main__':
    publish_data()



