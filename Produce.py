from kafka import KafkaProducer
from json import dumps
import json
from time import sleep
import pandas as pd
import psycopg2 


topic_name = 'hoang1'
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
            for index, row in ds_get.iterrows():
                get = [{'title': row['title'], 'id': row['id']}]
                get_s = pd.DataFrame(get)
                data = get_s.loc[df['id'].idxmax()].to_dict()
                producer.send(topic_name, value=data)
                del get_s
                print(str(data) + " sent")
            sleep(1)
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
