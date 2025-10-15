from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

HDFS_INPUT_PATH = "hdfs://23133070-master:9000/du_lieu/tmdb_movies_normalized.parquet"
HDFS_OUTPUT_PATH = "hdfs://23133070-master:9000/output/top_creator_highest_avg_rating"

mysql_host = "192.168.111.100"
mysql_port = "3306"
mysql_db = "movie_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
output_table = "top_creator_highest_avg_rating"

spark = SparkSession.builder \
    .appName("TopCreatorHighestAvgRating") \
    .config("spark.jars", "/usr/local/spark/jars/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

schema = StructType([
    StructField("Movie name", StringType(), True),
    StructField("Movie rate", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Creators", StringType(), True),
    StructField("Trailer link", StringType(), True),
    StructField("More Info", StringType(), True)
])

df = spark.read.schema(schema).parquet(HDFS_INPUT_PATH)
print(f"--- Đã đọc dữ liệu từ HDFS: {HDFS_INPUT_PATH} ---")

avg_rating_df = df.groupBy("Creators").agg(F.avg("Movie rate").alias("Average_Rating"))


window_spec = Window.partitionBy("Creators").orderBy(F.col("Movie rate").desc())
top_movie_df = df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

result_df = avg_rating_df.join(
    top_movie_df.select("Creators", F.col("Movie name").alias("Top_Movie_Name")),
    on="Creators",
    how="inner"
)

final_df = result_df.orderBy(F.col("Average_Rating").desc()).limit(1)
final_df.show(truncate=False)

final_df.coalesce(1).write.parquet(
    HDFS_OUTPUT_PATH,
    mode="overwrite"
)
print(f"--- Đã ghi kết quả ra HDFS: {HDFS_OUTPUT_PATH} ---")

final_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", output_table) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .mode("overwrite") \
    .save()

print(f"--- Đã ghi kết quả vào MySQL bảng: {output_table} ---")

spark.stop()

