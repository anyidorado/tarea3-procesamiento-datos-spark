from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

# 1. Crear la sesión de Spark
spark = SparkSession.builder.appName("Tarea3_TRM_Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el conjunto de datos desde HDFS
file_path = "hdfs://localhost:9000/Tarea3/query.csv"
print("\n=== 1. CARGA DEL CONJUNTO DE DATOS ===")
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)
print("Cantidad inicial de registros:", df.count())
df.printSchema()

# 3. Normalizar los nombres de las columnas
df = df.toDF(*[c.strip().lower() for c in df.columns])

# 4. Limpieza de datos
print("\n=== 2. LIMPIEZA DE DATOS ===")
# Eliminar filas completamente duplicadas
df = df.dropDuplicates()
# Convertir valor a tipo numérico y fechas a tipo fecha
df = df.withColumn(
    "valor",
    F.regexp_replace(F.trim(F.col("valor").cast("string")), ",", ".")
     .cast(DoubleType())
)
for columna in ["vigenciadesde", "vigenciahasta"]:
    if columna in df.columns:
        df = df.withColumn(
            columna,
            F.to_date(F.col(columna))
        )
# Eliminar registros sin valor TRM válido
df = df.filter(
    F.col("valor").isNotNull() & (F.col("valor") > 0)
)
print("Registros después de la limpieza:", df.count())
print("Valores nulos por columna:")
df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show(truncate=False)

# 5. Transformación de datos
print("\n=== 3. TRANSFORMACIÓN ===")
if "vigenciadesde" in df.columns:
    df = df.withColumn(
        "anio",
        F.year(F.col("vigenciadesde"))
    )
df.select("valor", "vigenciadesde", "vigenciahasta").show(10, truncate=False)

# 6. Análisis exploratorio de datos (EDA)
print("\n=== 4. ANÁLISIS EXPLORATORIO ===")
df.select("valor").describe().show()
print("Valores promedio, mínimo y máximo de la TRM:")
df.agg(
    F.avg("valor").alias("promedio_trm"),
    F.min("valor").alias("minimo_trm"),
    F.max("valor").alias("maximo_trm"),
    F.count("valor").alias("registros_validos")
).show(truncate=False)
print("Registros con TRM superior a 5000:")
df.filter(F.col("valor") > 5000) \
  .select("valor", "vigenciadesde", "vigenciahasta") \
  .show(20, truncate=False)
print("Valores ordenados de mayor a menor:")
df.orderBy(F.col("valor").desc()).show(20, truncate=False)

# 7. Resumen por año
if "vigenciadesde" in df.columns:
    print("Promedio de la TRM por año:")
    df.groupBy("anio") \
      .agg(
          F.round(F.avg("valor"), 2).alias("promedio_trm"),
          F.count("*").alias("cantidad_registros")
      ) \
      .orderBy("anio") \
      .show(100, truncate=False)
  
# 8. Finalizar la sesión
spark.stop()
print("\nProcesamiento batch finalizado.")
