from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, avg, min as spark_min, max as spark_max
)

spark = SparkSession.builder \
    .appName("ExtremePrecipitationTrend") \
    .getOrCreate()

# ----------------------------------
# 1. Leer CSVs
# ----------------------------------
df = spark.read.option("header", True).csv(
    "/home/julian/ghcn/raw/*.csv"
)

df = df.select(
    col("STATION"),
    col("LATITUDE").cast("double"),
    col("LONGITUDE").cast("double"),
    col("PRCP").cast("double"),
    year(col("DATE")).alias("year")
).filter(col("PRCP").isNotNull())

# ----------------------------------
# 2. Agregación anual por estación
# ----------------------------------
annual = df.groupBy(
    "STATION", "LATITUDE", "LONGITUDE", "year"
).agg(
    avg("PRCP").alias("avg_prcp")
)

# ----------------------------------
# 3. Obtener primer y último año
# ----------------------------------
bounds = annual.groupBy("STATION").agg(
    spark_min("year").alias("y0"),
    spark_max("year").alias("y1")
)

joined = annual.join(bounds, "STATION")

# ----------------------------------
# 4. Promedios inicio vs final
# ----------------------------------
trend_df = joined.groupBy(
    "STATION", "LATITUDE", "LONGITUDE"
).agg(
    avg(
        col("avg_prcp") * (col("year") == col("y1")).cast("int")
    ).alias("prcp_end"),
    avg(
        col("avg_prcp") * (col("year") == col("y0")).cast("int")
    ).alias("prcp_start"),
    spark_max("y1").alias("y1"),
    spark_min("y0").alias("y0")
)

# ----------------------------------
# 5. Pendiente
# ----------------------------------
result = trend_df.withColumn(
    "trend",
    (col("prcp_end") - col("prcp_start")) /
    (col("y1") - col("y0"))
).filter(col("trend").isNotNull())

# ----------------------------------
# 6. Guardar
# ----------------------------------
result.select(
    "STATION", "LATITUDE", "LONGITUDE", "trend"
).coalesce(1).write.mode("overwrite").csv(
    "/home/julian/ghcn/output/extreme_trend_by_station",
    header=True
)

spark.stop()
