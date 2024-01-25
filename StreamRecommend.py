from kafka import KafkaConsumer
import json
import pandas as pd
import streamlit as st
import plotly.express as px
from time import sleep


# Thiết lập các thông số kết nối đến Kafka broker
bootstrap_servers = 'localhost:9092'
group_id = 'my_consumer_group'
topic_name = 'hoang3'
# Tạo một KafkaConsumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'])

st.set_page_config(layout="wide")
placeholder = st.empty()

def visualdata(df):
    if len(df) >= 10:
        # Lấy số liệu của 10 hàng cuối cùng nếu có đủ phần tử
        last_10_rows = df.tail(10)
    else:
        # Nếu không đủ, lấy toàn bộ DataFrame
        last_10_rows = df
    with placeholder.container():
        # Layout của Streamlit
        st.title("Danh sách title và Biểu đồ tròn title_user_event")

        # Tạo hai cột với kích thước tùy chỉnh
        col1, col2 = st.columns([1, 2])  # Tỉ lệ kích thước 1:2

        # Phần trái: Hiển thị danh sách title của 10 hàng cuối cùng hoặc toàn bộ DataFrame
        with col1:
            st.header("Danh sách title")
            st.write(last_10_rows['title'].tolist())

        # Phần phải: Hiển thị bảng danh sách title_user và biểu đồ tròn title_user_event của 10 hàng cuối cùng hoặc toàn bộ DataFrame
        with col2:
            st.header("Danh sách title_user và Biểu đồ tròn title_user_event")
            
            # Hiển thị bảng danh sách title_user của 10 hàng cuối cùng hoặc toàn bộ DataFrame
            st.dataframe(last_10_rows[['title_user', 'title_user_event']], height=300)

            # Biểu đồ tròn title_user_event sử dụng Plotly của 10 hàng cuối cùng hoặc toàn bộ DataFrame
            fig = px.pie(last_10_rows, values='title_user_event', names='title_user', title='Biểu đồ tròn title_user_event')
            st.plotly_chart(fig)
            sleep(5)
    return 0

# Lặp để tiêu thụ các tin nhắn từ Kafka topic
flag = 0
flag_first = 0
const = 0
for message in consumer:
    try:
        placeholder.empty()
        json_data = message.value.decode('utf-8')
        data = json.loads(json_data)
        if const != 1:
            df = pd.DataFrame(columns=['title', 'id', 'title_user', 'title_user_event'])
            const  = 1
            flag = data['id']
        if data['id'] == flag:
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            const = 1
            visualdata(df)
        else:
            df = pd.DataFrame(columns=['title', 'id', 'title_user', 'title_user_event'])
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            const = 1
            flag = data['id']
            visualdata(df)

    except KeyboardInterrupt:
            print("break")
            break


    