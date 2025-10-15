from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- THÔNG TIN KẾT NỐI MYSQL (ĐIỂM ĐẾN) ---
# Sử dụng thông tin tương tự file 1 và 2
mysql_host = "192.168.111.100"
mysql_port = "3306"
mysql_db = "car_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"

# --- 1. Khởi tạo Spark Session ---
# Không cần Hive Support vì đọc trực tiếp từ HDFS và ghi ra MySQL
spark = SparkSession.builder \
    .appName("Top5CarsAnalysis") \
    .getOrCreate()

print("--- Spark Session đã được tạo ---")

# --- 2. ĐỌC DỮ LIỆU TỪ HDFS (INPUT) ---
# Dữ liệu parquet đã được chuẩn hóa, không cần đổi tên cột
hdfs_path = "hdfs://192.168.111.100:9000/user/hadoopdang/BigData/Car/car_data_normalized.parquet"
df = spark.read.parquet(hdfs_path)

print(f"--- Đã đọc dữ liệu từ HDFS: {hdfs_path} ---")

# --- 3. XỬ LÝ DỮ LIỆU ---
# Đếm số lượng xuất hiện của từng xe, lấy top 5
result_df = df.groupBy("car_name") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(5)

print("--- Xử lý dữ liệu hoàn tất, chuẩn bị ghi kết quả vào MySQL ---")
result_df.show()

# --- 4. GHI KẾT QUẢ VÀO MYSQL (OUTPUT) ---
output_table = "top_5_cars" # Tên bảng output trong MySQL

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
