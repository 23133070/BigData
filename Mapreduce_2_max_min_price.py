# mapreduce_2_max_min_price.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, max as spark_max, min as spark_min

# Khởi tạo Spark
spark = SparkSession.builder.appName("MaxMinPriceByYear").getOrCreate()

# Đọc CSV từ HDFS
df = spark.read.option("header","true").option("inferSchema","true") \
       .csv("hdfs://192.168.110.102:9000/input/car_data_clean.csv")


# Đổi tên cột
df = df.withColumnRenamed("năm_sản_xuất","year") \
       .withColumnRenamed("giá","price") \
       .withColumnRenamed("tên_xe","car_name")

# Chuyển price sang số
df = df.withColumn("price", F.regexp_replace(F.col("price").cast("string"), "[^0-9]", "").cast("long"))

# Bỏ bản ghi null
df = df.filter(F.col("year").isNotNull() & F.col("price").isNotNull())

# Tính max/min per year
ext = df.groupBy("year").agg(
    spark_max("price").alias("max_price"),
    spark_min("price").alias("min_price")
)

# Alias để tránh ambiguous column
df_alias = df.alias("d")
ext_alias = ext.alias("e")

# Max price cars
max_cars = df_alias.join(ext_alias, 
                         (df_alias.year == ext_alias.year) & (df_alias.price == ext_alias.max_price), 
                         "inner") \
                   .select(df_alias.year.alias("year"), 
                           df_alias.car_name.alias("max_car"), 
                           df_alias.price.alias("max_price")) \
                   .distinct()

# Min price cars
min_cars = df_alias.join(ext_alias, 
                         (df_alias.year == ext_alias.year) & (df_alias.price == ext_alias.min_price), 
                         "inner") \
                   .select(df_alias.year.alias("year"), 
                           df_alias.car_name.alias("min_car"), 
                           df_alias.price.alias("min_price")) \
                   .distinct()

# Join kết quả max/min
res = max_cars.join(min_cars, "year", "outer") \
              .select("year","max_car","max_price","min_car","min_price") \
              .orderBy("year")

# Lưu kết quả sang HDFS
res.write.mode("overwrite").parquet("hdfs://192.168.110.102:9000/output/max_min_price_by_year")

print("✅ MapReduce MaxMinPriceByYear đã chạy xong. Kiểm tra HDFS /output/max_min_price_by_year")

spark.stop()
