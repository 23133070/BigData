from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType

HDFS_INPUT_PATH = "hdfs://23133070-master:9000/du_lieu/tmdb_movies_normalized.parquet"
HDFS_OUTPUT_PATH = "hdfs://23133070-master:9000/output/rating_distribution_report"

mysql_host = "192.168.111.100"      
mysql_port = "3306"
mysql_db = "movie_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
output_table = "rating_distribution_report"


spark = SparkSession.builder \
    .appName("RatingDistributionToMySQL") \
    .config("spark.jars", "/usr/local/spark/jars/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

df = spark.read.parquet(HDFS_INPUT_PATH)
print(f"--- Đã đọc dữ liệu từ HDFS: {HDFS_INPUT_PATH} ---")

df_valid = df.filter(F.col("Movie rate").isNotNull())

total_movies = df_valid.count()
print(f"Tổng số phim hợp lệ: {total_movies}")

mapped_df = df_valid.withColumn("Rating Bucket",
    F.when((F.col("Movie rate") >= 1) & (F.col("Movie rate") < 4), "1-4")
     .when((F.col("Movie rate") >= 4) & (F.col("Movie rate") < 6), "4-6")
     .when((F.col("Movie rate") >= 6) & (F.col("Movie rate") < 8), "6-8")
     .when(F.col("Movie rate") >= 8, "8+")
     .otherwise("Unknown")
)

agg_df = mapped_df.groupBy("Rating Bucket").agg(
    F.count("*").alias("count"),
    F.sum("Movie rate").alias("sum_rating")
)

result_df = agg_df.withColumn("average_rating",
                    F.round(F.col("sum_rating") / F.col("count"), 3)) \
                  .withColumn("percentage",
                    F.round(F.col("count") / total_movies * 100, 2))

final_df = result_df.select(
    "Rating Bucket",
    "count",
    F.col("percentage").cast(DecimalType(5,2)),
    F.col("average_rating").cast(DecimalType(5,2))
).orderBy("Rating Bucket")

# --- Ghi ra HDFS (CSV) ---
final_df.coalesce(1).write.csv(
    HDFS_OUTPUT_PATH,
    mode="overwrite",
    header=True
)
print(f"Đã ghi ra HDFS tại: {HDFS_OUTPUT_PATH}")

final_df.write \
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


