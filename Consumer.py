import pandas as pd
import scipy.sparse as sparse
import numpy as np
import random
import implicit
from sklearn.preprocessing import MinMaxScaler
from sklearn import metrics
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

articles_df = pd.read_csv('Dataset/shared_articles.csv')
interactions_df = pd.read_csv('Dataset/users_interactions.csv')
articles_df.drop(['authorUserAgent', 'authorRegion', 'authorCountry'], axis=1, inplace=True)
interactions_df.drop(['userAgent', 'userRegion', 'userCountry'], axis=1, inplace=True)
articles_df = articles_df[articles_df['eventType'] == 'CONTENT SHARED']
articles_df.drop('eventType', axis=1, inplace=True)

df = pd.merge(interactions_df[['contentId','personId', 'eventType']], \
              articles_df[['contentId', 'title']], \
              how = 'inner', on = 'contentId')

event_type_strength = {
   'VIEW': 1.0,
   'LIKE': 2.0, 
   'BOOKMARK': 3.0, 
   'FOLLOW': 4.0,
   'COMMENT CREATED': 5.0,  
}

df['eventStrength'] = df['eventType'].apply(lambda x: event_type_strength[x])

df = df.drop_duplicates() # loai bo trung lap
grouped_df = df.groupby(['personId', 'contentId', 'title']).sum().reset_index()

grouped_df['title'] = grouped_df['title'].astype("category")
grouped_df['personId'] = grouped_df['personId'].astype("category")
grouped_df['contentId'] = grouped_df['contentId'].astype("category")
grouped_df['person_id'] = grouped_df['personId'].cat.codes
grouped_df['content_id'] = grouped_df['contentId'].cat.codes


sparse_content_person = sparse.csr_matrix((grouped_df['eventStrength'].astype(float), (grouped_df['content_id'], grouped_df['person_id'])))
sparse_person_content = sparse.csr_matrix((grouped_df['eventStrength'].astype(float), (grouped_df['person_id'], grouped_df['content_id'])))

model = implicit.als.AlternatingLeastSquares(factors=20, regularization=0.1, iterations=50)

alpha = 15
data = (sparse_content_person * alpha).astype('double')

# Fit the model
model.fit(data)

def recomend_system(content_id, n_similar = 10):

    person_vecs = model.user_factors
    content_vecs = model.item_factors

    content_norms = np.sqrt((content_vecs * content_vecs).sum(axis=1))

    scores = content_vecs.dot(content_vecs[content_id]) / content_norms
    top_idx = np.argpartition(scores, -n_similar)[-n_similar:]
    similar = sorted(zip(top_idx, scores[top_idx] / content_norms[content_id]), key=lambda x: -x[1])
    
    
    result_list = []
    
    for content in similar:
        idx, score = content
        result_list.append(grouped_df.title.loc[grouped_df.content_id == idx].iloc[0])
    return result_list

def get_top_events_for_person(person_id, num_events = 10):
    # Lọc các dòng có person_id tương ứng
    person_df = grouped_df[grouped_df['person_id'] == person_id]

    # Sắp xếp DataFrame theo 'eventStrength' theo thứ tự giảm dần
    person_df = person_df.sort_values(by='eventStrength', ascending=False)

    # Chọn 10 hàng đầu tiên
    top_events = person_df.head(num_events)[['title','eventType']]

    return top_events


scala_version = '2.12'  # your scala version
spark_version = '3.5.0' # your spark version
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.6.0' #your kafka version
]
spark = SparkSession.builder.master("local").appName("kafka-example").config("spark.jars.packages", ",".join(packages)).getOrCreate()
spark
packages

topic_name_receive = 'recommendsystem'
topic_name_sent = 'recommend'
kafka_server = 'localhost:9092'

kafkaDf = spark.read.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name_receive).option("startingOffsets", "earliest").load()
kafkaDf.toPandas()

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

kafka_schema = StructType([
     StructField("title", StringType(), True),
     StructField("id", StringType(), True),
])
# Chuyển đổi dữ liệu từ Kafka DataFrame sang DataFrame thông thường
json_column = from_json(col("value").cast("string"), kafka_schema).alias("json_data")
processed_df = kafkaDf.select(json_column).select("json_data.*")

# Hiển thị dữ liệu
processed_df.show(100)



ds_content = (df_content_transformed\
              .select(to_json(struct('user', 'timestamp', \
                                     'raw_comment', 'clean_comment',\
                                     'label')).alias('value'))\
              .selectExpr("CAST(value AS STRING)")
              .writeStream
              .format("kafka")
              .outputMode("append")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("topic", topic_name_sent)
              .option("checkpointLocation", "checkpoints/ds_content")
              .start())

ds_user = (df_user_tranformed\
              .select(to_json(struct('title', 'eventType', \
                                     'userid', 'timestamp',\
                                     )).alias('value'))\
              .selectExpr("CAST(value AS STRING)")
              .writeStream
              .format("kafka")
              .outputMode("append")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("topic", topic_name_sent)
              .option("checkpointLocation", "checkpoints/ds_user")
              .start())
