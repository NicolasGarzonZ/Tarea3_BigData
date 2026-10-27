from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

# 1. Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("BatchAnalysisTRM") \
    .getOrCreate()

# Ver los resultados limpios
spark.sparkContext.setLogLevel("WARN")

# 2. Cargar conjunto de datos desde el clúster HDFS

file_path = "hdfs://localhost:9000/Tarea3/rows.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# 3. Limpieza y Transformación de Datos (Eliminar nulos)
df_clean = df.dropna()

print("\n=== 1. ESQUEMA DEL DATASET ===")
df_clean.printSchema()

print("\n=== 2. MUESTRA DE REGISTROS CARGADOS ===")
df_clean.show(10, truncate=False)

# 4. Análisis Exploratorio de Datos 
print("\n=== 3. ESTADÍSTICAS DESCRIPTIVAS (SUMMARY) ===")
# Calcula media, desviación estándar, mínimos y máximos de la TRM
df_clean.summary().show()

print("\n=== 4. REGISTROS CON VALOR SUPERIOR A 5000 COP ===")
# Filtra los días donde el dólar estuvo por encima de 5000 pesos
df_clean.filter(F.col("VALOR") > 5000) \
    .select("VALOR", "VIGENCIADESDE", "VIGENCIAHASTA") \
    .show(10)

print("\n=== 5. MAYORES VALORES HISTÓRICOS REGISTRADOS ===")
# Ordena descendentemente para ver los picos históricos más altos
df_sorted = df_clean.sort(F.col("VALOR").desc())
df_sorted.show(10)


print("\nProcesamiento Batch finalizado con éxito.")
print("Manteniendo puerto 4040 activo por 60 segundos...")
# Este sleep te da tiempo de entrar a http://<TU_IP>:4040 en el navegador
# para tomar captura de los 'Jobs' y el DAG antes de que el script termine.
time.sleep(60)

# Finalizar la sesión
spark.stop()
