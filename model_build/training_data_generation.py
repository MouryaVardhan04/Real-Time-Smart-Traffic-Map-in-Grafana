import pandas as pd
import numpy as np

np.random.seed(42)
num_samples = 10000

road_ids = np.random.randint(1, 6, num_samples)
weather_opts = ['Clear', 'Cloudy', 'Rainy', 'Foggy']
weather = np.random.choice(weather_opts, num_samples)

data = pd.DataFrame({
    'road_id': road_ids,
    'avg_speed_kmph': np.random.uniform(15, 95, num_samples),
    'vehicle_count': np.random.randint(20, 240, num_samples),
    'occupancy_pct': np.random.uniform(10, 90, num_samples),
    'hour_of_day': np.random.randint(0, 24, num_samples),
    'day_of_week': np.random.randint(0, 7, num_samples),
    'weather': weather,
})

congestion = []
for _, row in data.iterrows():
    score = 0
    if row['avg_speed_kmph'] < 25:
        score += 2
    elif row['avg_speed_kmph'] < 45:
        score += 1
    if row['vehicle_count'] > 180:
        score += 2
    elif row['vehicle_count'] > 120:
        score += 1
    if row['occupancy_pct'] > 70:
        score += 2
    elif row['occupancy_pct'] > 50:
        score += 1
    if row['hour_of_day'] in range(8, 10) or row['hour_of_day'] in range(17, 19):
        score += 1
    if row['weather'] in ['Rainy', 'Foggy']:
        score += 1

    if score >= 5:
        label = 'HEAVY'
    elif score >= 3:
        label = 'MODERATE'
    else:
        label = 'LOW'
    congestion.append(label)

data['congestion_level'] = congestion
print(data.head())
data.to_csv('synthetic_traffic_data.csv', index=False)
