from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from config import configuration


# usage
def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key",configuration["AWS_ACCESS_KEY"])\
        .config("spark.hadoop.fs.s3a.secret.key", configuration["AWS_SECRET_KEY"])\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider")\
        .getOrCreate()

# Adjust the log level
# vehicle_schema
vehicleSchema = StructType([
    StructField(name="id", StringType(), nullable=True),
    StructField(name="deviceId", StringType(), nullable=True),
    StructField(name="timestamp", TimestampType(), nullable=True),
    StructField(name="location", StringType(), nullable=True),
    StructField(name="speed", DoubleType(), nullable=True),
    StructField(name="direction", StringType(), nullable=True),
    StructField(name="make", StringType(), nullable=True),
    StructField(name="model", StringType(), nullable=True),
    StructField(name="year", IntegerType(), nullable=True),
    StructField(name="fuelType", StringType(), nullable=True)
])
# gpsSchema
gpsSchema = StructType([
    StructField(name="id", StringType(), nullable=True),
    StructField(name="deviceId", StringType(), nullable=True),
    StructField(name="timestamp", TimestampType(), nullable=True),
    StructField(name="speed", DoubleType(), nullable=True),
    StructField(name="direction", StringType(), nullable=True),
    StructField(name="vehicleType", StringType(), nullable=True)
])

# trafficSchema
trafficSchema = StructType([
    StructField(name="id", StringType(), nullable=True),
    StructField(name="deviceId", StringType(), nullable=True),
    StructField(name="cameraId", StringType(), nullable=True),
    StructField(name="location", StringType(), nullable=True),
    StructField(name="timestamp", TimestampType(), nullable=True),
    StructField(name="snapshot", StringType(), nullable=True)
])


# emergencySchema
emergencySchema = StructType([
    StructField(name="id", StringType(), nullable=True),
    StructField(name="deviceId", StringType(), nullable=True),
    StructField(name="incidentId", StringType(), nullable=True),
    StructField(name="type", StringType(), nullable=True),
    StructField(name="timestamp", TimestampType(), nullable=True),
    StructField(name="location", StringType(), nullable=True),
    StructField(name="status", StringType(), nullable=True),
    StructField(name="description", StringType(), nullable=True)
])
# weatherSchema
weatherSchema = StructType([
    StructField(name="id", StringType(), nullable=True),
    StructField(name="deviceId", StringType(), nullable=True),
    StructField(name="location", StringType(), nullable=True),
    StructField(name="timestamp", TimestampType(), nullable=True),
    StructField(name="temperature", DoubleType(), nullable=True),
    StructField(name="weatherCondition", StringType(), nullable=True),
    StructField(name="precipitation", DoubleType(), nullable=True),
    StructField(name="windSpeed", DoubleType(), nullable=True),
    StructField(name="humidity", IntegerType(), nullable=True),
    StructField(name="airQualityIndex", DoubleType(), nullable=True)
])
def read_kafka_topic(topic, schema):
    return (spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'broker:29092')
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .load()
        .selectExpr('CAST(value AS STRING)')
        .select(from_json(col('value'), schema).alias('data'))
        .select('data.*')
        .withWatermark('timestamp', '2 minutes')
    )


def streamWriter(input: DataFrame, checkpointFolder, output):
    return (input.writeStream
        .format('parquet')
        .option('checkpointLocation', checkpointFolder)
        .option('path', output)
        .outputMode('append')
        .start())


vehicleDF = read_kafka_topic(topic='vehicle_data', schema=vehicleSchema).alias('vehicle')

gpsDF = read_kafka_topic(topic='gps_data',schema= gpsSchema).alias('gps')

trafficDF = read_kafka_topic(topic='traffic_data',schema= trafficSchema).alias('traffic')

weatherDF = read_kafka_topic(topic='weather_data',schema= weatherSchema).alias('weather')

emergencyDF = read_kafka_topic(topic='emergency_data', schema=emergencySchema).alias('emergency')

query1=streamWriter(vehicleDF, checkpointFolder='s3a://s3-spark-streaming-data/checkpoints/vehicle_data',
             output='s3a://s3-spark-streaming-data/data/vehicle_data')

query2=streamWriter(gpsDF, checkpointFolder='s3a://s3-spark-streaming-data/checkpoints/gps_data',
             output='s3a://s3-spark-streaming-data/data/gps_data')

query3=streamWriter(trafficDF, checkpointFolder='s3a://s3-spark-streaming-data/checkpoints/traffic_data',
             output='s3a://s3-spark-streaming-data/data/traffic_data')

query4=streamWriter(weatherDF, checkpointFolder='s3a://s3-spark-streaming-data/checkpoints/weather_data',
             output='s3a://s3-spark-streaming-data/data/weather_data')

query5=streamWriter(emergencyDF, checkpointFolder='s3a://s3-spark-streaming-data/checkpoints/emergency_data',
             output='s3a://s3-spark-streaming-data/data/emergency_data')

query5.awaitTermination()

if __name__ == "__main__":
    main()
