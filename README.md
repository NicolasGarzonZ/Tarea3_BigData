# Análisis de Big Data: Volatilidad de la TRM en Colombia (Spark & Kafka)

## Descripción de la Solución
Este proyecto implementa una arquitectura de Big Data para el análisis de la Tasa de Cambio Representativa del Mercado (TRM) en Colombia. La solución está dividida en dos componentes principales:
1. **Procesamiento en Batch:** Utiliza Apache Spark (PySpark) y HDFS para ingestar, limpiar y realizar un Análisis Exploratorio de Datos (EDA) sobre el histórico de la TRM obtenido de Datos Abiertos de Colombia. Se identifican valores atípicos y se calculan estadísticas descriptivas utilizando abstracciones de *DataFrames*.
2. **Procesamiento en Tiempo Real:** Implementa Apache Kafka y Spark Structured Streaming para simular y procesar telemetría financiera. Un productor en Python inyecta cotizaciones de dólares de múltiples entidades financieras, mientras que un consumidor en Spark calcula métricas agregadas (promedio de tasa y volumen transado) mediante ventanas de tiempo (*Time Windows*) de 1 minuto.

## Tecnologías Utilizadas
* **Almacenamiento Distribuido:** Apache Hadoop (HDFS)
* **Procesamiento Analítico:** Apache Spark (PySpark, Catalyst Optimizer)
* **Ingesta de Eventos:** Apache Kafka, Zookeeper
* **Lenguaje:** Python 3

## Instrucciones de Ejecución

### 1. Requisitos Previos
* Iniciar el clúster de Hadoop y asegurar que el archivo de datos esté en HDFS:
Inicia todos los servicios de almacenamiento y gestión:
   `start-all.sh`
  `hdfs dfs -put rows.csv /Tarea3/rows.csv`
* Iniciar Zookeeper y el servidor de Kafka en segundo plano:
  `sudo /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties`
  `sudo /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties`
* Crear el tópico de Kafka:
  `/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic transacciones_trm`

### 2. Ejecución del Procesamiento en Batch
Ejecutar el script de análisis histórico, el cual imprimirá los esquemas, estadísticas descriptivas (summary) y las consultas ordenadas directamente en la consola:
```bash
 `python3 batch_analysis.py`

## Instrucciones de Ejecución simulacion

### 1. Ejecución Producer

 `python3 kafka_producer.py`

### 2. Ejecución Consumer

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py`
