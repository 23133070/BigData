from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("AvgPriceByOrigin").getOrCreate()

df = spark.read.option("header","true").csv("hdfs://192.168.110.102:9000/input/car_data_clean.csv")
df = df.withColumnRenamed("xuất_xứ","origin").withColumnRenamed("giá","price")

df = df.withColumn("price", F.regexp_replace(F.col("price").cast("string"), "[^0-9]", "").cast("long"))
df = df.filter(F.col("origin").isNotNull() & F.col("price").isNotNull())

res = df.groupBy("origin").agg(F.round(F.avg("price"),2).alias("avg_price"), F.count("*").alias("count")) \
        .orderBy(F.desc("avg_price"))

#res.show(200, truncate=False)

res.write.mode("overwrite").parquet("hdfs://192.168.110.102:9000/output/avg_price_by_origin")

print("MapReduce AvgPriceByOrigin đã chạy xong. Kiểm tra HDFS /output/avg_price_by_origin")

spark.stop()
