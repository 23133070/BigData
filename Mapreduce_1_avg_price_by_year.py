from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType # Import công cụ ép kiểu

# --- THÔNG TIN KẾT NỐI MYSQL (ĐIỂM ĐẾN) ---
mysql_host = "192.168.111.100"
mysql_port = "3306"
mysql_db = "car_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"

# --- 1. Khởi tạo Spark Session ---
spark = SparkSession.builder \
    .appName("HdfsToMySqlAnalysis") \
    .getOrCreate()

print("--- Spark Session đã được tạo ---")

# --- 2. ĐỌC DỮ LIỆU TỪ HDFS (INPUT) ---
hdfs_path = "hdfs://192.168.111.100:9000/user/hadoopdang/BigData/Car/car_data_normalized.parquet"
df = spark.read.parquet(hdfs_path)

print(f"--- Đã đọc dữ liệu từ HDFS: {hdfs_path} ---")

# --- 3. XỬ LÝ DỮ LIỆU (ĐÃ SỬA LỖI) ---
result_df = df.filter(F.col("year").isNotNull() & F.col("price").isNotNull()) \
    .groupBy("year").agg(
        # Ép kiểu avg_price thành Decimal để tương thích với MySQL
        F.avg("price").cast(DecimalType(38, 2)).alias("avg_price"),
        F.count("*").alias("car_count")
    ).orderBy(F.col("year").desc())

print("--- Xử lý dữ liệu hoàn tất, chuẩn bị ghi kết quả vào MySQL ---")
result_df.show()

# --- 4. GHI KẾT QUẢ VÀO MYSQL (OUTPUT) ---
output_table     = "avg_price_by_year"

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
