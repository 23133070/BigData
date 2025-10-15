from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- THÔNG TIN KẾT NỐI MYSQL ---
mysql_host = "192.168.111.100"
mysql_port = "3306"
mysql_db = "car_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"

# --- 1. Khởi tạo Spark Session ---
spark = SparkSession.builder \
    .appName("MaxMinPriceAnalysis") \
    .getOrCreate()

print("--- Spark Session đã được tạo ---")

# --- 2. ĐỌC DỮ LIỆU TỪ HDFS ---
hdfs_path = "hdfs://192.168.111.100:9000/user/hadoopdang/BigData/Car/car_data_normalized.parquet"
df = spark.read.parquet(hdfs_path)
print(f"--- Đã đọc dữ liệu từ HDFS: {hdfs_path} ---")

# --- 3. Lọc dữ liệu hợp lệ ---
df_filtered = df.filter(
    F.col("year").isNotNull() &
    F.col("price").isNotNull() &
    F.col("car_name").isNotNull()
)

# --- 4. Tính giá min/max theo năm ---
price_extremes = df_filtered.groupBy("year").agg(
    F.max("price").alias("max_price"),
    F.min("price").alias("min_price")
)

# --- 5. Alias DataFrame để tránh ambiguous column ---
df_alias = df_filtered.alias("df")
price_alias = price_extremes.alias("pe")

# Lấy xe giá cao nhất mỗi năm
max_cars = df_alias.join(
    price_alias,
    (F.col("df.year") == F.col("pe.year")) & (F.col("df.price") == F.col("pe.max_price")),
    "inner"
).select(
    F.col("df.year").alias("year"),
    F.col("df.car_name").alias("max_price_car"),
    F.col("df.price").alias("max_price")
).distinct()

# Lấy xe giá thấp nhất mỗi năm
min_cars = df_alias.join(
    price_alias,
    (F.col("df.year") == F.col("pe.year")) & (F.col("df.price") == F.col("pe.min_price")),
    "inner"
).select(
    F.col("df.year").alias("year"),
    F.col("df.car_name").alias("min_price_car"),
    F.col("df.price").alias("min_price")
).distinct()

# --- 6. Gộp kết quả ---
result_df = max_cars.join(min_cars, "year", "outer") \
    .select("year", "max_price_car", "max_price", "min_price_car", "min_price") \
    .orderBy(F.col("year").desc())

print("--- Xử lý dữ liệu hoàn tất, chuẩn bị ghi kết quả vào MySQL ---")
result_df.show()

# --- 7. Ghi kết quả vào MySQL ---
output_table = "max_min_price_by_year"

result_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", output_table) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .mode("overwrite") \
    .save()

print(f"--- HOÀN TẤT! Đã lưu kết quả vào bảng MySQL: {output_table} ---")
spark.stop()
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
