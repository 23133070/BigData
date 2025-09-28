# spark_main_full.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, max as spark_max, min as spark_min

spark = SparkSession.builder.appName("CarAnalysisFull").getOrCreate()

df = spark.read.csv("hdfs://192.168.110.102:9000/input/car_data_clean.csv", header=True, inferSchema=True)
# rename columns
df = df.withColumnRenamed("năm_sản_xuất","year") \
       .withColumnRenamed("giá","price") \
       .withColumnRenamed("tên_xe","car_name") \
       .withColumnRenamed("xuất_xứ","origin")

df = df.withColumn("price", F.regexp_replace(F.col("price").cast("string"), "[^0-9]", "").cast("long"))
df = df.filter(F.col("year").isNotNull() & F.col("price").isNotNull())

# 1. avg price by year
avg_by_year = df.groupBy("year").agg(F.round(F.avg("price"),2).alias("avg_price"), F.count("*").alias("count")).orderBy("year")
avg_by_year.show(50, truncate=False)
avg_by_year.write.mode("overwrite").parquet("hdfs://localhost:9000/output/avg_price_by_year")

# 2. max/min by year with car names
ext = df.groupBy("year").agg(spark_max("price").alias("max_price"), spark_min("price").alias("min_price"))
max_cars = df.join(ext, (df.year==ext.year) & (df.price==ext.max_price)).select(df.year.alias("year"), df.car_name.alias("max_car"), df.price.alias("max_price")).distinct()
min_cars = df.join(ext, (df.year==ext.year) & (df.price==ext.min_price)).select(df.year.alias("year"), df.car_name.alias("min_car"), df.price.alias("min_price")).distinct()
max_min = max_cars.join(min_cars, "year", "outer").select("year","max_car","max_price","min_car","min_price").orderBy("year")
max_min.show(200, truncate=False)
max_min.write.mode("overwrite").parquet("hdfs://localhost:9000/output/max_min_price_by_year")

# 3. top5 cars
top5 = df.groupBy("car_name").count().orderBy(F.desc("count")).limit(5)
top5.show(20, truncate=False)
top5.write.mode("overwrite").parquet("hdfs://localhost:9000/output/top5_cars")

# 4. avg price by origin
avg_origin = df.groupBy("origin").agg(F.round(F.avg("price"),2).alias("avg_price"), F.count("*").alias("count")).orderBy(F.desc("avg_price"))
avg_origin.show(200, truncate=False)
avg_origin.write.mode("overwrite").parquet("hdfs://localhost:9000/output/avg_price_by_origin")

print(" All jobs done.")
spark.stop()
