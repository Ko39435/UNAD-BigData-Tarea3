from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos JSON recibidos
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Lectura del flujo continuo desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Procesamiento del JSON
sensor_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Agregación en tiempo real por sensor
aggregated_df = sensor_df.groupBy("sensor_id") \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Mostrar resultados por consola
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
