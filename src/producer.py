import json
import time
import os
from datetime import datetime, timezone
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'sensor-events'

def delivery_callback(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_producer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'sensor-producer',
        'acks': 'all',
    }
    return Producer(conf)

def generate_sensor_data(sensor_id: int):
    """Generate realistic mock sensor reading."""
    return {
        'sensor_id': f'sensor-{sensor_id:03d}',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'temperature_c': round(20.0 + (sensor_id % 15) + (time.time() % 5), 2),
        'humidity_percent': round(40.0 + (sensor_id % 30), 1),
        'pressure_hpa': round(1013.0 + (sensor_id % 10 - 5), 1),
        'event_type': 'reading',
        'location': 'factory-floor-1'
    }

def main():
    producer = create_producer()
    print(f"Starting producer. Sending events to topic '{TOPIC}'...")
    print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    for i in range(20):  # Send 20 events for demo
        data = generate_sensor_data(i)
        producer.produce(
            topic=TOPIC,
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0.1)
        print(f"Sent event {i+1}/20: {data['sensor_id']} - Temp: {data['temperature_c']}°C")
        time.sleep(0.5)
    
    producer.flush()
    print("Producer finished. All events sent.")

if __name__ == "__main__":
    main()
