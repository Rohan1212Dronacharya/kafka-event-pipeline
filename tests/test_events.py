import json
from datetime import datetime, timezone
from src.producer import generate_sensor_data, create_producer
from src.consumer import process_event

def test_generate_sensor_data():
    """Test 1: Sensor data generation produces valid structure."""
    data = generate_sensor_data(42)
    assert isinstance(data, dict)
    assert 'sensor_id' in data
    assert 'timestamp' in data
    assert 'temperature_c' in data
    assert 'humidity_percent' in data
    assert data['sensor_id'] == 'sensor-042'
    assert isinstance(data['temperature_c'], float)
    assert 15.0 <= data['temperature_c'] <= 45.0
    assert isinstance(data['humidity_percent'], float)

def test_process_event():
    """Test 2: Event processing adds metadata and applies business logic."""
    sample_event = {
        'sensor_id': 'sensor-005',
        'timestamp': '2026-10-04T12:00:00Z',
        'temperature_c': 28.5,
        'humidity_percent': 65.0,
        'pressure_hpa': 1015.2,
        'event_type': 'reading',
        'location': 'factory-floor-1'
    }
    
    processed = process_event(sample_event)
    
    assert processed['status'] == 'processed'
    assert 'processed_at' in processed
    assert isinstance(processed['alert'], bool)
    assert processed['alert'] is False  # 28.5 < 35
    assert processed['sensor_id'] == sample_event['sensor_id']

def test_high_temperature_alert():
    """Test 3: High temperature triggers alert flag."""
    hot_event = {
        'sensor_id': 'sensor-999',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'temperature_c': 42.0,
        'humidity_percent': 50.0,
        'pressure_hpa': 1010.0,
        'event_type': 'reading'
    }
    
    processed = process_event(hot_event)
    assert processed['alert'] is True
    assert processed['temperature_c'] == 42.0

def test_json_serialization():
    """Test 4: Events can be serialized/deserialized without data loss."""
    original = generate_sensor_data(17)
    serialized = json.dumps(original)
    deserialized = json.loads(serialized)
    
    assert deserialized['sensor_id'] == original['sensor_id']
    assert deserialized['temperature_c'] == original['temperature_c']
    assert 'timestamp' in deserialized

def test_producer_config():
    """Test 5: Producer can be instantiated with valid config."""
    producer = create_producer()
    assert producer is not None
    # Basic smoke test - config is valid if object created
    assert hasattr(producer, 'produce')
