from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, avg, substring

spark = SparkSession.builder \
    .appName("HottestYearByCountry") \
    .getOrCreate()

# Leer datos
df = spark.read.option("header", True).csv("file:///home/julian/ghcn/raw/*.csv")

# Cast
df = df.withColumn("TMAX", col("TMAX").cast("double")) \
       .withColumn("year", year(col("DATE"))) \
       .withColumn("country", substring(col("STATION"), 1, 2))

# Promedio anual de TMAX
avg_year = df.groupBy("country", "year") \
    .agg(avg("TMAX").alias("avg_tmax"))

# Año más caluroso por país
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("country").orderBy(desc("avg_tmax"))

result = avg_year.withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Guardar para visualización
result.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("file:///home/julian/ghcn/output/hottest_year_by_country")

spark.stop()
