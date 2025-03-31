import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid
import time


TORONTO_COORDINATES = {"latitude": 43.39, "longitude": -79.23}
LONDON_COORDINATES = {"latitude": 42.89, "longitude": -81.24}

# Calculate movement increment
LATITUDE_INCREMENT = (LONDON_COORDINATES['latitude'] - TORONTO_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (LONDON_COORDINATES['longitude'] - TORONTO_COORDINATES['longitude']) / 100

# Environment variables for Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_camera_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_incident_data')

# Global variables
start_time = datetime.now()
start_location = TORONTO_COORDINATES.copy()


def get_next_time():
    """Generates the next timestamp with random increments."""
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # Update frequently
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    """Generates mock GPS data."""
    return {
        'id': str(uuid.uuid4()),  # Ensure UUID is serialized properly
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': round(random.uniform(10, 40), 2),
        'direction': 'South-West',
        'vehicle_type': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    """Generates mock traffic camera data."""
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'camera_id': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp, location):
    """Generates mock weather data."""
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': round(random.uniform(-20, 30), 2),
        'weatherCondition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Snow']),  # Fixed bug
        'precipitation': round(random.uniform(0, 25), 2),
        'windSpeed': round(random.uniform(0, 100), 2),
        'humidity': round(random.uniform(0, 100), 2),  # Percentage
        'airQualityIndex': round(random.uniform(0, 500), 2)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    """Generates mock emergency incident data."""
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'incidentId': str(uuid.uuid4()),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'This is a description of the emergency situation'
    }


def json_serializer(obj):
    """Serializes JSON objects."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    """Handles message delivery reports."""
    if err is not None:
        print(f'Message Delivery Failed: {err}')
    else:
        print(f'Message Delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    """Sends data to Kafka topics."""
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()  # Call flush only once per batch 

def journey_vehicle_movement():
    """Simulates vehicle movement towards London."""
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    """Generates mock vehicle data."""
    location = journey_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': round(random.uniform(10, 40), 2),
        'direction': 'South-West',
        'make': 'BMW',
        'model': 'C500',
        'year': '2025',
        'fuel_type': 'Hybrid'
    }


def simulate_journey(producer, device_id):
    """Simulates the vehicle journey and streams data to Kafka."""
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='camera123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if abs(vehicle_data['location'][0] - LONDON_COORDINATES['latitude']) < 0.01 and \
           abs(vehicle_data['location'][1] - LONDON_COORDINATES['longitude']) < 0.01:
            print('Vehicle has reached London. Simulation is ending...')
            break
        #print(vehicle_data)
        #print(gps_data)
        #print(weather_data)
        #print(emergency_incident_data)
        #print(traffic_camera_data)
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        

        time.sleep(5)


if __name__ == "__main__":
    print(f'Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}')

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'vehicle-codewithms-123')
    except KeyboardInterrupt:
        print('Simulation ended by user')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
