from confluent_kafka import Producer
from config import config
import pandas as pd

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} and sent to partition {event.partition()}.')
   
def taxi(producer, key):
    value = f'{key} taxi CSV data for {year} month {month:02} processed'
    producer.produce('taxi_csv_topic'_csv', value, key, on_delivery=callback)
    
if __name__ == '__main__':
    producer = Producer(config)
    keys = ['fhv', 'green']
    [taxi(producer, key) for key in keys]
    producer.flush()
    

    
    
    
 
