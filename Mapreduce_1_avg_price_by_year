from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("AvgPriceByYear").getOrCreate()

# Đọc CSV từ HDFS
df = spark.read.csv("hdfs://192.168.110.102:9000/input/car_data_clean.csv", 
                    header=True, inferSchema=True)

# Đổi tên cột
df = df.withColumnRenamed("năm_sản_xuất", "year") \
       .withColumnRenamed("giá", "price")

# Chuyển giá sang số, loại bỏ ký tự lạ
df = df.withColumn("price", F.regexp_replace(F.col("price").cast("string"), "[^0-9]", "").cast("long"))

# Bỏ các bản không có year hoặc price
df = df.filter(F.col("year").isNotNull() & F.col("price").isNotNull())

# Tính giá trung bình và số lượng xe theo năm
result = df.groupBy("year").agg(
    F.round(F.avg("price"), 2).alias("avg_price"),
    F.count("*").alias("count")
).orderBy("year")

#result.show(50, truncate=False)

# Lưu kết quả sang HDFS (parquet)
result.write.mode("overwrite").parquet("hdfs://192.168.110.102:9000/output/avg_price_by_year")

print(" MapReduce AvgPriceByYear đã chạy xong. Kiểm tra HDFS /output/avg_price_by_year")

spark.stop()
