from confluent_kafka import Consumer, KafkaException
from config import config
import pandas as pd
from prefect import flow, task

def set_consumer_configs():
    config['group.id'] = 'hello_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False
    
    
def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')
        

if __name__ == '__main__':
    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe(['taxi_csv_topic'], on_assign=assignment_callback)
    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf8')
                partition = event.partition()
                print(f'{val} received from partition {partition}    ')
                consumer.commit(event)
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        consumer.close()
