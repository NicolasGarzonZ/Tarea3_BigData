from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisTRM_RealTime") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema basado en el mercado de divisas
schema = StructType([
    StructField("entidad_id", IntegerType()),
    StructField("tasa_cambio", FloatType()),
    StructField("monto_usd", FloatType()),
    StructField("timestamp", LongType())
])

# 3. Leer el streaming desde el nuevo tópico de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacciones_trm") \
    .load()

# 4. Parsear JSON y convertir el tiempo
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# 5. Analítica en tiempo real (Promedio de TRM y suma de Dólares por ventana de 1 min)
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "entidad_id") \
    .agg({
        "tasa_cambio": "avg",
        "monto_usd": "sum"
    })

# 6. Escribir resultados en consola
query = windowed_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
