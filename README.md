# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

**Asignatura:** Big Data  
**Institución:** Universidad Nacional Abierta y a Distancia (UNAD)  
**Programa:** Ingeniería de Sistemas  
**Autor:** Kevin Nicolas Ortega Urbano  

---

## 📌 Descripción del Proyecto

Este repositorio contiene la solución práctica de la **Tarea 3 - Procesamiento de Datos con Apache Spark**, correspondiente al curso de Big Data. El proyecto abarca dos componentes principales:

1. **Procesamiento en Lote (*Batch*):** Carga, limpieza, análisis exploratorio de datos (EDA) y transformaciones avanzadas sobre un conjunto de datos de ventas mediante **PySpark (DataFrames y RDDs)**.
2. **Procesamiento en Tiempo Real (*Streaming*):** Arquitectura pub/sub con **Apache Kafka** y **Spark Streaming**, procesando y agregando eventos en tiempo real recibidos desde sensores simulados.

---

## 📁 Estructura del Repositorio

* **`tarea3_batch.py`**: Script en PySpark para procesamiento en lote. Implementa transformaciones en DataFrames (`withColumn`, `groupBy`, `agg`) y operaciones nativas con RDDs (`filter`, `map`, `count`, `take`).
* **`kafka_producer.py`**: Script en Python que actúa como productor de eventos, simulando lecturas de datos de sensores (temperatura y humedad) y enviándolos en formato JSON al *topic* `sensor_data` de Kafka.
* **`spark_streaming_consumer.py`**: Script en PySpark Streaming que consume los eventos del *topic* de Kafka en tiempo real, parsea la estructura JSON y calcula promedios continuos agrupados por sensor.
* **`ventas.csv`**: Dataset estático utilizado para el procesamiento por lotes.

---

## 🚀 Instrucciones de Ejecución

### Prerrequisitos
* Java JDK 8 o superior.
* Apache Spark (v3.x).
* Apache Kafka y ZooKeeper.
* Python 3 con las librerías `pyspark` y `kafka-python`.

---

### 1. Ejecución del Procesamiento Batch

Para ejecutar el análisis por lotes con PySpark:


python3 tarea3_batch.py

2. Ejecución del Procesamiento en Tiempo Real (Kafka + Spark Streaming)
Abrir terminales independientes y seguir el orden de inicio:

Paso 1: Iniciar ZooKeeper (Terminal 1)

sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
Paso 2: Iniciar el Broker de Apache Kafka (Terminal 2)

sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
Paso 3: Iniciar el Productor de Eventos (Terminal 3)

python3 kafka_producer.py
Paso 4: Iniciar el Consumidor de Spark Streaming (Terminal 4)

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_streaming_consumer.py
