from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, udf, current_timestamp, expr
import pandas as pd
import pickle

spark = SparkSession.builder \
    .appName("KafkaTrafficPrediction") \
    .getOrCreate()


def get_timescale_connection():
    return {
        "url": "jdbc:postgresql://timescaledb:5432/traffic_detection",
        "driver": "org.postgresql.Driver",
        "user": "user",
        "password": "password"
    }


schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("road_id", IntegerType(), True),
    StructField("road_name", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed_kmph", DoubleType(), True),
    StructField("occupancy_pct", DoubleType(), True),
    StructField("weather", StringType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("severity", StringType(), True),
])

with open('traffic_detection_model.pkl', 'rb') as model_file:
    model_pipeline = pickle.load(model_file)


@udf(StringType())
def predict_congestion_udf(road_id, avg_speed_kmph, vehicle_count, occupancy_pct, hour_of_day, day_of_week, weather):
    model_input = {
        'road_id': road_id,
        'avg_speed_kmph': avg_speed_kmph,
        'vehicle_count': vehicle_count,
        'occupancy_pct': occupancy_pct,
        'hour_of_day': hour_of_day,
        'day_of_week': day_of_week,
        'weather': weather,
    }
    df = pd.DataFrame([model_input])
    try:
        prediction = model_pipeline.predict(df)[0]
        return str(prediction)
    except Exception:
        if avg_speed_kmph is None:
            return 'LOW'
        if avg_speed_kmph < 30 or (occupancy_pct is not None and occupancy_pct > 75):
            return 'HEAVY'
        if avg_speed_kmph < 50 or (occupancy_pct is not None and occupancy_pct > 55):
            return 'MODERATE'
        return 'LOW'


traffic_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "raw_traffic") \
    .load()

traffic_data = traffic_stream.selectExpr("CAST(value AS STRING)")
traffic_json = traffic_data.select(from_json(col("value"), schema).alias("traffic_data"))
traffic_df = traffic_json.select("traffic_data.*")

road_metadata_df = spark.read \
    .format("jdbc") \
    .option("url", get_timescale_connection()['url']) \
    .option("dbtable", "road_metadata") \
    .option("user", "user") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

road_metadata_df = road_metadata_df.select("road_id", "road_name", "latitude", "longitude", "city")
road_metadata_df.cache()

joined_df = traffic_df.join(road_metadata_df.select("road_id", "city"), "road_id", "left")
joined_df = joined_df.withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("day_of_week", col("day_of_week").cast("string")) \
    .withColumn(
        "predicted_congestion",
        predict_congestion_udf(
            col("road_id"),
            col("avg_speed_kmph"),
            col("vehicle_count"),
            col("occupancy_pct"),
            col("hour_of_day"),
            col("day_of_week"),
            col("weather")
        )
    ).withColumn("prediction_timestamp", current_timestamp()) \
     .withColumn("target_timestamp", expr("CAST(timestamp AS TIMESTAMP) + INTERVAL 5 MINUTES"))

traffic_output = joined_df.select(
    "event_id", "road_id", "road_name", "timestamp", "latitude", "longitude",
    "vehicle_count", "avg_speed_kmph", "occupancy_pct", "weather", "day_of_week",
    "hour_of_day", "event_type", "source", "incident_type", "severity"
)

prediction_output = joined_df.select(
    "road_id", "prediction_timestamp", "target_timestamp",
    "predicted_congestion", "source"
)


def write_to_db(batch_df, batch_id):
    traffic_batch = batch_df.select(
        "event_id", "road_id", "road_name", col("timestamp").cast("timestamp").alias("timestamp"), "latitude", "longitude",
        "vehicle_count", "avg_speed_kmph", "occupancy_pct", "weather", col("day_of_week").cast("string").alias("day_of_week"),
        "hour_of_day", "event_type", "source", "incident_type", "severity"
    )
    if not traffic_batch.rdd.isEmpty():
        traffic_batch.write \
            .format("jdbc") \
            .option("url", get_timescale_connection()['url']) \
            .option("dbtable", "traffic_events") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    prediction_batch = batch_df.select(
        "road_id", col("prediction_timestamp").cast("timestamp").alias("prediction_timestamp"),
        col("target_timestamp").cast("timestamp").alias("target_timestamp"),
        "predicted_congestion", "source"
    )
    if not prediction_batch.rdd.isEmpty():
        prediction_batch.write \
            .format("jdbc") \
            .option("url", get_timescale_connection()['url']) \
            .option("dbtable", "traffic_predictions") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()


joined_df.writeStream \
    .option("checkpointLocation", "/tmp/traffic_stream_checkpoint") \
    .foreachBatch(write_to_db) \
    .start() \
    .awaitTermination()
