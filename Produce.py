import pandas as pd
import scipy.sparse as sparse
import numpy as np
import random
import implicit
from sklearn.preprocessing import MinMaxScaler
from sklearn import metrics
from kafka import KafkaProducer
from json import dumps
import json
from time import sleep
import pandas as pd
import psycopg2 



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
    top_events = person_df.head(num_events)[['title','eventStrength']]

    return top_events


topic_name = 'hoang3'
kafka_server = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))


# Connect to the database 
conn = psycopg2.connect(database="dataeng", user="postgres", 
                        password="postgres", host="localhost", port="5432") 

# create a cursor 
cur = conn.cursor() 

# create const
flag = 0 

for e in range(1000):
    try:
        # Excute get data from database
        cur.execute( 
            '''SELECT * FROM ACTIONS ORDER BY id DESC LIMIT 1;''') 
        data_from_db = cur.fetchall()
        # commit the changes 
        conn.commit() 

        # Get data
        posts = [
            {
                'id': row[0],
                'userid': row[1],
                'contentid': row[2],
                'title': str(row[3]),
                'type': str(row[4]),
                'timestamp': str(row[5])
            }
            for row in data_from_db
        ]
        df = pd.DataFrame(posts)
        const = df[['contentid','userid']]
        if const['contentid'].iloc[0] != flag:
            result = recomend_system(const['contentid'].iloc[0])
            result_user = get_top_events_for_person(const['userid'].iloc[0])
            ds_get = pd.DataFrame({'title' : result, 'id' : const['userid'].iloc[0]})
            ds_get_user = result_user.reset_index(drop=True)
            ds_get['title_user'] = ds_get_user['title'].combine_first(ds_get['title'])
            ds_get['event'] = 0
            ds_get['title_user_event'] = ds_get_user['eventStrength'].combine_first(ds_get['event'])
            for index, row in ds_get.iterrows():
                get = [{'title': row['title'], 'id': row['id'], 'title_user': row['title_user'], 'title_user_event' : row['title_user_event']}]
                get_s = pd.DataFrame(get)
                data = get_s.loc[df['id'].idxmax()].to_dict()
                producer.send(topic_name, value=data)
                del get_s
                print(str(data) + " sent")
                sleep(5)
            flag = const['contentid'].iloc[0]
        else:
            sleep(1)
    except KeyboardInterrupt:
            print("break")
            break
# close the cursor and connection 
cur.close() 
conn.close()   
producer.flush()
