import time
import random
import uuid
from kafka import KafkaProducer
import json
import numpy as np
from datetime import datetime


class IncidentProducer:
    def __init__(self, brokers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic

    def produce_incident_events(self):
        city_roads = [
            (1, 'Main Avenue', 12.9716, 77.5946),
            (2, 'Ring Road', 12.9650, 77.5836),
            (3, 'City Center Blvd', 12.9751, 77.5937),
            (4, 'Old Airport Road', 12.9862, 77.6060),
            (5, 'Tech Park Expressway', 12.9248, 77.6110),
        ]
        incident_types = ['RoadClosure', 'Accident', 'SlowTraffic', 'SignalFailure']
        weather_types = ['Rainy', 'Foggy', 'Cloudy']

        while True:
            road_id, road_name, lat, lon = random.choice(city_roads)
            incident_type = random.choice(incident_types)
            traffic_event = {
                'event_id': str(uuid.uuid4()),
                'road_id': road_id,
                'road_name': road_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'latitude': round(lat + random.uniform(-0.003, 0.003), 6),
                'longitude': round(lon + random.uniform(-0.003, 0.003), 6),
                'vehicle_count': int(np.random.uniform(120, 240)),
                'avg_speed_kmph': round(np.random.uniform(8, 25), 2),
                'occupancy_pct': round(np.random.uniform(70, 95), 2),
                'weather': random.choice(weather_types),
                'day_of_week': datetime.now().isoweekday(),
                'hour_of_day': datetime.now().hour,
                'event_type': 'incident',
                'incident_type': incident_type,
                'severity': random.choice(['LOW', 'MEDIUM', 'HIGH']),
                'source': 'synthetic',
            }

            self.producer.send(self.topic, value=traffic_event)
            print(f"Produced incident: {traffic_event}")
            time.sleep(3)


if __name__ == '__main__':
    brokers = ['broker:29092']
    topic = 'raw_traffic'

    producer = IncidentProducer(brokers, topic)
    producer.produce_incident_events()