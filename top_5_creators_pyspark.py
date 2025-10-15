from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

HDFS_INPUT_PATH = "hdfs://23133070-master:9000/du_lieu/tmdb_movies_normalized.parquet"
HDFS_OUTPUT_PATH = "hdfs://23133070-master:9000/output/top_5_creators_count"

mysql_host = "192.168.111.100"
mysql_port = "3306"
mysql_db = "movie_analysis_db"
mysql_user = "sqoopdang"
mysql_password = "1"
jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
output_table = "top_5_creators_count"

spark = SparkSession.builder \
    .appName("Top5CreatorsToMySQL") \
    .config("spark.jars", "/usr/local/spark/jars/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

print("--- Spark session đã khởi tạo ---")

schema = StructType([
    StructField("Movie name", StringType(), True),
    StructField("Movie rate", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Creators", StringType(), True),
    StructField("Trailer link", StringType(), True),
    StructField("More Info", StringType(), True)
])

df = spark.read.parquet(HDFS_INPUT_PATH, schema=schema)
print(f"--- Đã đọc dữ liệu từ HDFS: {HDFS_INPUT_PATH} ---")

creator_counts = (
    df.groupBy("Creators")
      .agg(F.count("*").alias("Total_Movies"))
)

window = Window.partitionBy("Creators").orderBy(F.col("Movie rate").desc())
df_with_rank = df.withColumn("rank", F.row_number().over(window))
top_movies_per_creator = (
    df_with_rank.filter(F.col("rank") == 1)
                .select("Creators", 
                        F.col("Movie name").alias("Top_Rated_Movie"),
                        F.col("Movie rate").alias("Top_Rate"))
)

result = (
    creator_counts.join(top_movies_per_creator, on="Creators", how="inner")
                  .orderBy(F.col("Total_Movies").desc())
                  .limit(5)
)

result.coalesce(1).write.parquet(
    HDFS_OUTPUT_PATH,
    mode="overwrite"
)
print(f"--- Đã ghi kết quả ra HDFS: {HDFS_OUTPUT_PATH} ---")

result.write \
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

