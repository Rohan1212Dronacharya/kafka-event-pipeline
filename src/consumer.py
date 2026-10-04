import json
import os
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'sensor-events'
OUTPUT_FILE = '/app/output/events.log'

def create_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'sensor-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    return Consumer(conf)

def process_event(data: dict):
    """Process a sensor event (could write to DB, alert, aggregate, etc.)."""
    processed = {
        **data,
        'processed_at': datetime.now(timezone.utc).isoformat(),
        'status': 'processed',
        'alert': data['temperature_c'] > 35.0  # Example business rule
    }
    return processed

def main():
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    
    print(f"Consumer started. Listening to topic '{TOPIC}'...")
    print(f"Events will be logged to {OUTPUT_FILE}")
    
    try:
        event_count = 0
        while event_count < 25:  # Run until we process enough events
            msg = consumer.poll(timeout=5.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                processed = process_event(data)
                
                # Log to file (simulating persistent storage)
                with open(OUTPUT_FILE, 'a') as f:
                    f.write(json.dumps(processed, indent=2) + "\n---\n")
                
                print(f"Processed event from {data['sensor_id']}: {data['temperature_c']}°C - Alert: {processed['alert']}")
                event_count += 1
                
            except json.JSONDecodeError:
                print("Failed to decode message")
    
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        consumer.close()
        print(f"Consumer shut down. Processed {event_count} events. Check {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
