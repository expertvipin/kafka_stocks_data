import json 
import pandas as pd
from kafka import KafkaConsumer
from database import create_table,insert_data

conf = {
    'bootstrap_servers':'localhost:9092',
    'value_deserializer':lambda v: v.decode('utf-8'),
    'enable_auto_commit':True,
}

consumer = KafkaConsumer('Topic1',**conf)

def consume_data(data):
    create_table()

    for each in consumer:
        data = json.loads(each.value).get('Time Series (5min)')
        for values in data.items():
            insert_data(data=values[1],date= values[0])

consume_data(data=None)
