from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, avg

spark = SparkSession.builder \
    .appName("GHCN-Ejercicio1-TMAX") \
    .getOrCreate()

# 1. Cargar CSV
df = spark.read.option("header", True).csv(
    "file:///home/julian/ghcn/raw/*.csv"
)

# 2. Filtrar TMAX válido
tmax = df.filter(col("TMAX").isNotNull())

# 3. Convertir unidades (décimas °C → °C)
tmax = tmax.withColumn(
    "tmax_c",
    col("TMAX").cast("double") / 10.0
)

# 4. Extraer año
tmax = tmax.withColumn(
    "year",
    substring(col("DATE"), 1, 4)
)

# 5. País desde ID estación
tmax = tmax.withColumn(
    "country",
    substring(col("STATION"), 1, 2)
)

# 6. Promedio anual por país
annual_avg = tmax.groupBy(
    "country", "year"
).agg(
    avg("tmax_c").alias("avg_tmax")
)

# 7. Año más caluroso por país
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("country").orderBy(col("avg_tmax").desc())

result = annual_avg.withColumn(
    "rank",
    row_number().over(w)
).filter(col("rank") == 1)

result.select("country", "year", "avg_tmax").show(truncate=False)

spark.stop()
