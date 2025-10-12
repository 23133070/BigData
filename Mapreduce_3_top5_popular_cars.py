from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Top5Cars").getOrCreate()

# Đọc Parquet từ HDFS
df = spark.read.parquet("hdfs://192.168.110.105:9000/user/phuquy/BigData_Project/StandardizedDataCar/car_data_normalized.parquet")

df = df.withColumnRenamed("tên_xe","car_name")

res = df.groupBy("car_name").count().orderBy(F.desc("count")).limit(5)
#res.show(20, truncate=False)

res.write.mode("overwrite").parquet("hdfs://192.168.110.105:9000/output/top5_cars")

print(" MapReduce Top5Cars đã chạy xong. Kiểm tra HDFS /output/top5_cars")

spark.stop()
