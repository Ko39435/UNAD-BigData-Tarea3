
import json
import time
import random
from kafka import KafkaProducer

# Conexión al servidor local de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    return {
        'sensor_id': random.randint(1, 5),
        'temperature': round(random.uniform(20.0, 35.0), 2),
        'humidity': round(random.uniform(30.0, 70.0), 2),
        'timestamp': int(time.time())
    }

print("Iniciando envío de datos a Kafka (Topic: sensor_data)...")

try:
    while True:
        sensor_data = generate_sensor_data()
        producer.send('sensor_data', value=sensor_data)
        print(f"Sent: {sensor_data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProductor detenido.")
    producer.close()

