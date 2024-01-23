from flask import Flask, render_template, request, redirect, url_for, session
import psycopg2 
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'glightIE221'

# Connect to the database 
conn = psycopg2.connect(database="dataeng", user="postgres", 
                        password="postgres", host="localhost", port="5432") 

# create a cursor 
cur = conn.cursor() 
  
# if you already have any table or not id doesnt matter this  
# will create a login and action table for you. 
cur.execute( 
    '''CREATE TABLE IF NOT EXISTS login (id SERIAL PRIMARY KEY ,iduser INT );''') 
cur.execute( 
    '''CREATE TABLE IF NOT EXISTS actions (id SERIAL PRIMARY KEY, userid INT,contentid INT, title varchar(250), type varchar(250) , timestamp time);''') 


# Insert some data into the table 

# commit the changes 

# get data from database
cur.execute("SELECT * FROM contentbe")
data_from_db = cur.fetchall()

# commit the changes 
conn.commit() 
  
# close the cursor and connection 
cur.close() 
conn.close()   

# Dữ liệu mẫu về bài viết
posts = [{'title': str(row[0]), 'content': str(row[1])} for row in data_from_db]



@app.route('/')
def home():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username')
    password = request.form.get('password')
    session['userid'] = username
    return redirect('/content')

@app.route('/content')
def index():
    return render_template('index.html', posts=posts)




@app.route('/actionstype', methods=['POST'])
def content():
    conn = psycopg2.connect(database="dataeng", user="postgres", 
                        password="postgres", host="localhost", port="5432") 
    
    # create a cursor 
    cur = conn.cursor() 

    # get time 
    current_time = datetime.now()
    current_time = current_time.strftime('%H:%M:%S')
    # get userid
    userid = session.get('userid', None)

    # get data click
    data = request.get_json()
    # Content
    click_content = data.get('content')
    click_content = str(click_content)[:250]
    # title
    click_title = data.get('id')
    click_title = int(click_title)
    # type
    click_type = data.get('type')
    click_type = str(click_type)[:250]
    
    # Insert data
    cur.execute( 
        '''INSERT INTO actions (userid, contentid, title, type,timestamp) VALUES (%s,%s, %s, %s, %s)''', 
        (userid, click_title, click_content, click_type, current_time)) 
    
    # commit the changes 
    conn.commit() 

    return redirect(url_for('index')) 


if __name__ == '__main__':
    app.run(debug=True)
