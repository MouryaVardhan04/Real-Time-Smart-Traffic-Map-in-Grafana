
# Real-Time Traffic Detection System

## Overview

This project implements a real-time traffic detection and anomaly monitoring system using Kafka, Spark Structured Streaming, machine learning, TimescaleDB, and Grafana. It detects risky traffic patterns by processing live road activity data from Kafka, applying a predictive model, and storing the results for monitoring through a Grafana dashboard.

---

## Components

The system consists of the following components:

1. **Kafka Broker**: Ingests live road traffic events and distributes them to consumers.
2. **Zookeeper**: Manages the Kafka broker.
3. **Producer**:
   - **Normal Producer**: Generates regular traffic events and sends them to the Kafka topic `raw_traffic`.
   - **Hacker Producer**: Simulates suspicious traffic patterns that will be flagged by the model.
4. **Spark Cluster**:
   - **Spark Master** and **Spark Workers**: Run a Spark Structured Streaming job that reads traffic event data from Kafka, applies feature extraction, and infers violation probability using a pre-trained model.
5. **TimescaleDB**: A PostgreSQL-based time-series database for storing processed traffic data and violation predictions.
6. **Grafana Dashboard**: Visualizes traffic activity and anomaly metrics in real time.
7. **Model Build**: Contains scripts and synthetic data to retrain the traffic detection model.

---

## System Architecture Diagram

The system ingests live traffic events from Kafka, predicts congestion and incident risk with Spark + ML, stores results in TimescaleDB, and visualizes them in Grafana.

---

## Prerequisites

Before running the project, ensure that you have the following installed:

- Docker
- Docker Compose
- Python 3.x for model retraining (optional)

---

## Setup and Usage

### 1. Clone the Repository
```bash
git clone https://github.com/MouryaVardhan04/Real-Time-Smart-Traffic-Map-in-Grafana.git
cd Real-Time-Smart-Traffic-Map-in-Grafana
```

### 2. Build and Run the Docker Containers
Use Docker Compose to build and start the containers.

```bash
docker-compose up --build -d
```

This command starts all the services defined in the `docker-compose.yml`, including Kafka, Spark, TimescaleDB, and Grafana.

### 3. Producers
There are two producers for generating traffic data:
- **Normal Producer**: Generates regular traffic events every second.
- **Hacker Producer**: Generates suspicious traffic patterns intermittently to simulate real-world anomalies.

Both producers send the event data to the Kafka topic `raw_traffic`.

### 4. Spark Structured Streaming
The Spark job processes incoming traffic events from Kafka, creates features, and runs inference using a pre-trained machine learning model. The results (violation predictions) are written to TimescaleDB for further analysis.

### 5. Database (TimescaleDB)
TimescaleDB stores the processed traffic data and violation predictions. A database schema is initialized using the SQL file located at `./monitoring/init.sql`.

### 6. Grafana Dashboard
Grafana is used to visualize real-time traffic anomaly metrics. Once the system is running, you can access the Grafana dashboard at:
```
http://localhost:3000
```
Login credentials (default):
- Username: `admin`
- Password: `admin`

Navigate to Dashboards and click on the Traffic Detection Dashboard.

Here, you'll be able to monitor traffic activity and detect suspicious movement trends over a 10-minute period.

Note - You may need to refresh the dashboard for the first time. Just refresh the dashboard page for the first time and later it will update after every 5 seconds.

## Grafana Dashboard

The dashboard visualizes traffic speed, congestion, occupancy, and incident counts in near real time.

---

## Model Training and Retraining

If you want to retrain the traffic detection model, follow these steps:

1. **Train the Model**:
   The `model_build` directory contains the code to generate synthetic traffic data and train a machine learning model. You can use the provided `ml_model.py` script to train a new model using synthetic traffic data.

   ```bash
   cd model_build
   python ml_model.py
   ```

   This will generate a new model binary file `traffic_detection_model.pkl`.

2. **Update the Model**:
   After training, move the new model binary from the `model_build` directory to the `data_processor` directory to replace the existing model used by the Spark Structured Streaming job.

   ```bash
   mv traffic_detection_model.pkl ../data_processor/traffic_detection_model.pkl
   ```

3. **Restart the Spark Job**:
   After updating the model, restart the Spark Structured Streaming job to load the updated model.

   ```bash
   docker-compose restart spark-worker
   ```

This process allows the system to use a new machine learning model for traffic anomaly detection.

---

## Directory Structure

```bash
traffic-detection-system/
├── data_producer/               # Kafka producers (normal and incident producers)
│   ├── Dockerfile
│   ├── hacker_traffic_producer.py
│   ├── normal_traffic_producer.py
│   ├── requirements.txt
│   └── wait-for-it.sh
├── data_processor/              # Spark Structured Streaming job
│   ├── Dockerfile.spark
│   ├── traffic_detection_model.pkl
│   └── traffic_detector.py      # The traffic detection script
├── model_build/                 # Machine learning model building scripts
│   ├── ml_model.py              # Script to train the model
│   ├── requirements.txt
│   ├── synthetic_traffic_data.csv
│   └── training_data_generation.py
├── monitoring/                  # TimescaleDB and Grafana configurations
│   ├── grafana-dashboard/       
│   │   ├── dashboard-provider.yaml
│   │   ├── datasource.yaml
│   │   ├── Dockerfile           # Grafana setup
│   │   └── traffic_detection_dashboard.json
│   ├── init.sql                 # SQL file to initialize TimescaleDB schema
├── venv/                        # Python virtual environment (optional)
├── .gitignore
├── docker-compose.yml           # Docker Compose configuration
├── LICENSE
└── README.md                    # Project documentation
```

---

## Key Technologies

- **Kafka**: Message broker to handle streaming data.
- **Spark Structured Streaming**: For real-time data processing.
- **TimescaleDB**: Time-series database built on PostgreSQL for storing traffic event data.
- **Grafana**: Dashboard to visualize the real-time traffic anomaly metrics.
- **Docker**: Containerizes all components for easy deployment.

---

## Future Improvements

- **Model Retraining Automation**: Automate the process of moving the model binary to the `data_processor` directory and restarting the Spark job.
- **Scaling**: Introduce horizontal scaling to support larger traffic volumes.
- **Enhanced Monitoring**: Add more detailed metrics to Grafana for better insights into system performance.

---

## License

This project is licensed under the MIT License.

---

## Contributing

Feel free to open issues or create pull requests to enhance the project.

---

## Contact

For any inquiries, please contact [iit.ravindra@gmail.com](mailto:iit.ravindra@gmail.com).

