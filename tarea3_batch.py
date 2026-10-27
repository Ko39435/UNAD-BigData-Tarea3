
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchVentasUNAD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n--- 1. CARGA DEL DATASET ---")
df = spark.read.csv("ventas.csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()

print("\n--- 2. LIMPIEZA Y TRANSFORMACIONES (DataFrames) ---")
# Crear columna calculada: Venta Total (precio * cantidad)
df_transformado = df.withColumn("total_venta", col("precio_unitario") * col("cantidad"))
df_transformado.show(5)

print("\n--- 3. ANÁLISIS EXPLORATORIO Y AGREGACIONES (EDA) ---")
# A. Ventas totales y promedio por Categoría
ventas_categoria = df_transformado.groupBy("categoria") \
    .agg(
        sum("total_venta").alias("total_ventas_usd"),
        round(avg("total_venta"), 2).alias("promedio_venta_usd"),
        count("id_transaccion").alias("num_transacciones")
    ) \
    .orderBy(col("total_ventas_usd").desc())

print("Ventas por Categoria:")
ventas_categoria.show()

# B. Ventas por Sucursal
ventas_sucursal = df_transformado.groupBy("sucursal") \
    .agg(
        sum("total_venta").alias("total_ventas_usd"),
        sum("cantidad").alias("total_productos_vendidos")
    ) \
    .orderBy(col("total_ventas_usd").desc())

print("Ventas por Sucursal:")
ventas_sucursal.show()

print("\n--- 4. USO DE OPERACIONES CON RDDs ---")
# Convertir DataFrame a RDD para cumplir el requerimiento de RDDs de la guía
rdd_ventas = df_transformado.rdd

# Transformaciones RDD (map, filter) y Acciones (take, count, reduce)
rdd_filtrado = rdd_ventas.filter(lambda row: row["total_venta"] > 100)
rdd_mapeado = rdd_filtrado.map(lambda row: (row["categoria"], row["total_venta"]))

print(f"Total de registros con ventas > $100 (Accion Count): {rdd_filtrado.count()}")
print("Muestra de datos en RDD (Accion Take):", rdd_mapeado.take(3))

# Guardar resultado procesado en CSV
ventas_categoria.coalesce(1).write.mode("overwrite").option("header", "true").csv("resultado_batch_ventas")

print("\n--- PROCESAMIENTO BATCH FINALIZADO CON ÉXITO ---")
spark.stop()
