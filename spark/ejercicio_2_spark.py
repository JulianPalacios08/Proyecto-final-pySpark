from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, sum as spark_sum

# -----------------------------
# Configuración Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("ExtremePrecipitationDetection") \
    .getOrCreate()

# -----------------------------
# Parámetros
# -----------------------------
INPUT_PATH = "/home/julian/ghcn/raw/*.csv"
OUTPUT_PATH = "/home/julian/ghcn/output/extreme_prcp_by_station"

CURRENT_YEAR = 2025
YEARS_BACK = 10
PRCP_THRESHOLD = 500  # 50 mm = 500 décimas

# -----------------------------
# Lectura de datos
# -----------------------------
df = spark.read.option("header", True).csv(INPUT_PATH)

# -----------------------------
# Limpieza y tipos
# -----------------------------
df = df.withColumn("DATE", to_date(col("DATE"), "yyyy-MM-dd")) \
       .withColumn("PRCP", col("PRCP").cast("double")) \
       .withColumn("LATITUDE", col("LATITUDE").cast("double")) \
       .withColumn("LONGITUDE", col("LONGITUDE").cast("double"))

# -----------------------------
# Filtrado (última década + PRCP extrema)
# -----------------------------
df_filtered = df.filter(
    (year(col("DATE")) >= (CURRENT_YEAR - YEARS_BACK)) &
    (col("PRCP") > PRCP_THRESHOLD)
)

# -----------------------------
# MAP: (station, lat, lon) → 1
# REDUCE: suma
# -----------------------------
result = df_filtered.groupBy(
    "STATION", "LATITUDE", "LONGITUDE"
).agg(
    spark_sum(col("PRCP") * 0 + 1).alias("extreme_days")
)

# -----------------------------
# Ordenar (más anómalas primero)
# -----------------------------
result = result.orderBy(col("extreme_days").desc())

# -----------------------------
# Guardar resultado
# -----------------------------
result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_PATH)

spark.stop()
