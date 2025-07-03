from config import kafka_config
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=kafka_config.KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
def send_producer(obj):
    try:
        producer.send(kafka_config.KAFKA_TOPIC, obj)
        producer.flush()
        print('Enviou para o kafka')
    except Exception as e:
        print(e)

