from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

HDFS_INPUT_PATH = "hdfs://192.168.12.101:9000/du_lieu/tmdb_movies_normalized.parquet"
HDFS_OUTPUT_PATH = "hdfs://192.168.12.101:9000/output/top_20_movies_by_rate"

mysql_host = "192.168.111.100"      
mysql_port = "3306"
mysql_db = "movie_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
output_table = "top_20_movies_by_rate"

spark = SparkSession.builder \
    .appName("Top20MoviesToMySQL") \
    .config("spark.jars", "/usr/local/spark/jars/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

print("--- Spark session đã khởi tạo ---")

schema = StructType([
    StructField("Movie name", StringType(), True),
    StructField("Movie rate", FloatType(), True),
    StructField("Description", StringType(), True),
    StructField("Creators", StringType(), True),
    StructField("Trailer link", StringType(), True),
    StructField("More Info", StringType(), True)
])

df = spark.read.parquet(HDFS_INPUT_PATH, header=True, schema=schema)
print(f"--- Đã đọc dữ liệu từ HDFS: {HDFS_INPUT_PATH} ---")

top_20_movies = (
    df.orderBy(F.col("Movie rate").desc())
      .select("Movie name", "Movie rate", "Creators")
      .limit(20)
)

top_20_movies.coalesce(1).write.parquet(
    HDFS_OUTPUT_PATH,
    mode="overwrite"
)
print(f"Đã ghi kết quả ra HDFS: {HDFS_OUTPUT_PATH}")

top_20_movies.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", output_table) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .mode("overwrite") \
    .save()

print(f"Đã ghi kết quả vào MySQL bảng: {output_table}")

spark.stop()