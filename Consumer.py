import findspark
findspark.init()
import pyspark
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from IPython.display import display, clear_output
from pyspark.sql.streaming import DataStreamReader
scala_version = '2.12'  # your scala version
spark_version = '3.5.0' # your spark version
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.6.0' #your kafka version
]
spark = SparkSession.builder.master("local").appName("kafka-example").config("spark.jars.packages", ",".join(packages)).getOrCreate()
spark

packages

topic_name_receive = 'hoang3'
topic_name_sent = 'recommend'
kafka_server = 'localhost:9092'

# BATCH TIME
# kafkaDf = spark.read.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name_receive).option("startingOffsets", "earliest").load()

# kafka_schema = StructType([
#      StructField("title", StringType(), True),
#      StructField("id", StringType(), True),
#      StructField("title_user", StringType(), True),
# ])
# # Chuyển đổi dữ liệu từ Kafka DataFrame sang DataFrame thông thường
# json_column = from_json(col("value").cast("string"), kafka_schema).alias("json_data")
# processed_df = kafkaDf.select(json_column).select("json_data.*")

# # Hiển thị dữ liệu
# processed_df.show(100)

#STREAM DATA

df = (spark.readStream.format('kafka')
      .option("kafka.bootstrap.servers", "localhost:9092") 
      .option("subscribe", topic_name_receive) 
      .option("startingOffsets", "latest")
      .load())

schema_value = StructType([
     StructField("title", StringType(), True),
     StructField("id", StringType(), True),
     StructField("title_user", StringType(), True),
])

df_json = (df
           .selectExpr("CAST(value AS STRING)")
           .withColumn("value",f.from_json("value",schema_value)))

df_column = (df_json.select(f.col("value.id").alias("id"),
#                             f.col("value.date").alias("timestamp"),
                           f.col("value.title").alias("title"),
                           f.col("value.title_user").alias("title_user"),
                           ))

ds = (df_column
      .select(f.to_json(f.struct('id','title',
                                    'title_user')).alias('value'))
      .selectExpr("CAST(value AS STRING)") 
      .writeStream 
      .format("kafka") 
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092") 
      .option("topic", topic_name_sent) 
      .option("checkpointLocation","checkpoints/df_column")
      .start())
ds.awaitTermination()