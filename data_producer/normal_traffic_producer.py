import time
import random
import uuid
from kafka import KafkaProducer
import json
import numpy as np
from datetime import datetime


class TrafficProducer:
    def __init__(self, brokers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic

    def produce_traffic_events(self):
        city_roads = [
            (1, 'Main Avenue', 12.9716, 77.5946, 'LOW'),
            (2, 'Ring Road', 12.9650, 77.5836, 'MID'),
            (3, 'City Center Blvd', 12.9751, 77.5937, 'HIGH'),
            (4, 'Old Airport Road', 12.9862, 77.6060, 'MID'),
            (5, 'Tech Park Expressway', 12.9248, 77.6110, 'LOW'),
        ]
        weather_types = ['Clear', 'Cloudy', 'Rainy', 'Foggy']

        while True:
            road_id, road_name, lat, lon, _ = random.choice(city_roads)
            hour_of_day = datetime.now().hour
            vehicle_count = max(10, int(np.random.normal(120, 35)))
            base_speed = 55 if hour_of_day in range(8, 10) or hour_of_day in range(17, 19) else 72
            avg_speed_kmph = round(max(10, min(120, np.random.normal(base_speed, 12))), 2)
            occupancy_pct = round(max(5, min(95, np.random.normal(35 + (vehicle_count / 10), 15))), 2)
            if hour_of_day in range(8, 10) or hour_of_day in range(17, 19):
                occupancy_pct = min(95, occupancy_pct + 15)
                avg_speed_kmph = max(10, avg_speed_kmph - 12)

            traffic_event = {
                'event_id': str(uuid.uuid4()),
                'road_id': road_id,
                'road_name': road_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'latitude': round(lat + random.uniform(-0.002, 0.002), 6),
                'longitude': round(lon + random.uniform(-0.002, 0.002), 6),
                'vehicle_count': int(vehicle_count),
                'avg_speed_kmph': avg_speed_kmph,
                'occupancy_pct': occupancy_pct,
                'weather': random.choice(weather_types),
                'day_of_week': datetime.now().isoweekday(),
                'hour_of_day': hour_of_day,
                'event_type': 'normal',
                'source': 'synthetic',
            }

            self.producer.send(self.topic, value=traffic_event)
            print(f"Produced: {traffic_event}")
            time.sleep(1)


if __name__ == '__main__':
    brokers = ['broker:29092']
    topic = 'raw_traffic'

    producer = TrafficProducer(brokers, topic)
    producer.produce_traffic_events()