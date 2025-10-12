# mapreduce_2_max_min_price.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, max as spark_max, min as spark_min

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MaxMinPriceByYear").getOrCreate()

# Đọc dữ liệu Parquet từ HDFS
df = spark.read.parquet("hdfs://192.168.110.105:9000/user/phuquy/BigData_Project/StandardizedDataCar/car_data_normalized.parquet")

# Đổi tên các cột để dễ xử lý
df = df.withColumnRenamed("năm_sản_xuất", "year") \
       .withColumnRenamed("giá", "price") \
       .withColumnRenamed("tên_xe", "car_name")

# Chuyển cột price sang kiểu số (loại bỏ ký tự không phải số)
df = df.withColumn("price", F.regexp_replace(F.col("price").cast("string"), "[^0-9]", "").cast("long"))

# Loại bỏ bản ghi thiếu hoặc không hợp lệ
df = df.dropna(subset=["year", "price"]) \
       .filter((F.col("year") > 1900) & (F.col("price") > 0))

# Tính giá cao nhất và thấp nhất theo năm
ext = df.groupBy("year").agg(
    spark_max("price").alias("max_price"),
    spark_min("price").alias("min_price")
)

# Tạo alias để tránh trùng tên cột
df_alias = df.alias("d")
ext_alias = ext.alias("e")

# Lấy các xe có giá cao nhất theo năm
max_cars = df_alias.join(
    ext_alias,
    (df_alias.year == ext_alias.year) & (df_alias.price == ext_alias.max_price),
    "inner"
).select(
    df_alias.year.alias("year"),
    df_alias.car_name.alias("max_car"),
    df_alias.price.alias("max_price")
).distinct()

# Lấy các xe có giá thấp nhất theo năm
min_cars = df_alias.join(
    ext_alias,
    (df_alias.year == ext_alias.year) & (df_alias.price == ext_alias.min_price),
    "inner"
).select(
    df_alias.year.alias("year"),
    df_alias.car_name.alias("min_car"),
    df_alias.price.alias("min_price")
).distinct()

# Gộp kết quả max/min lại với nhau
res = max_cars.join(min_cars, "year", "outer") \
              .select("year", "max_car", "max_price", "min_car", "min_price") \
              .orderBy("year")

# Ghi kết quả ra HDFS
res.write.mode("overwrite").parquet("hdfs://192.168.110.105:9000/output/max_min_price_by_year")

print("MapReduce MaxMinPriceByYear đã chạy xong. Kiểm tra HDFS /output/max_min_price_by_year để xem kết quả.")

# Dừng Spark
spark.stop()
