# Importing the libraries
import boto3
import pymysql
from flask import Flask, render_template, request, session, flash, redirect
from werkzeug.utils import secure_filename
from db import *

# Initializing the Flask app
app = Flask(__name__)
app.secret_key = 'secret_key'

AWS_Access_Key_ID = 'AKIAX4AXRHWAKSBZKSHH'
AWS_Secret_Access_Key = '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'
AWS_Bucket_Name = 'multiweekfileupload'
AWS_Bucket_Region = 'us-east-2'

s3 = boto3.client('s3', region_name=AWS_Bucket_Region ,aws_access_key_id=AWS_Access_Key_ID,aws_secret_access_key=AWS_Secret_Access_Key)
sns = boto3.client('sns', region_name=AWS_Bucket_Region ,aws_access_key_id=AWS_Access_Key_ID,aws_secret_access_key=AWS_Secret_Access_Key)

def create_database():
    try:
        connection = pymysql.connect(host=database_endpoint, user=database_user, password=database_password, database=database_name)
        db_cursor = connection.cursor()
        db_cursor.execute("USE defaultdb;")
        db_cursor.execute("create table if not exists lokiuserrdetails (username varchar(255), email varchar(255) unique, password varchar(255));")
        db_cursor.execute("create table if not exists lokifileupload (email varchar(255), filename varchar(255));")
        connection.commit()
        print("Table created successfully")
    except Exception as e:
        print("Exception occurred", e)

def check_user(email):
    try:
        connection = pymysql.connect(host=database_endpoint, user=database_user, password=database_password, database=database_name)
        db_cursor = connection.cursor()
        db_cursor.execute("USE defaultdb;")
        db_cursor.execute("select * from lokiuserrdetails where email='%s';" % (email))
        result = db_cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as e:
        print("Exception occurred", e)

def check_bucket_exists():
    try:
        s3.head_bucket(Bucket=AWS_Bucket_Name)
    except:
        s3.create_bucket(Bucket=AWS_Bucket_Name, CreateBucketConfiguration={'LocationConstraint': AWS_Bucket_Region})

def create_sns_topic():
    try:
        sns_create = sns.create_topic(Name='filedonwloadtopic')
        return sns_create['TopicArn']
    except:
        print("Topic already exists")

@app.route('/')
def index():
    return render_template('signin.html')

@app.route('/signin', methods=['GET','POST'])
def signin():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        connection_cursor = pymysql.connect(host=database_endpoint, user=database_user, password=database_password, database=database_name)
        db_cursor = connection_cursor.cursor()
        try:
            user_details = db_cursor.execute("select email, password from lokiuserrdetails where email=%s;", (email))
            if user_details:
                user_details = db_cursor.fetchone()
                if user_details[1] == password:
                    session['email'] = email
                    return render_template('fileupload.html')
                else:
                    warning = "Incorrect password! Try again.."
                    return render_template('signin.html', warning=warning)
            else:
                warning = "Email not found! Please sign up or check your email."
                return render_template('signin.html', warning=warning)
        except Exception as e:
            print("Exception occurred", e)
            return render_template('signin.html')
        
    return render_template('signin.html')  # This will handle the 'GET' method and also any unexpected paths in 'POST'

@app.route('/fileupload', methods=['POST'])
def fileupload():
    if 'file' not in request.files:
        flash("No File is selected.")
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash("No File is attached.")
        return redirect(request.url)
    
    email_address = []
    for i in range(1, 3):
        email_key = "email" + str(i)
        if email_key in request.form and request.form[email_key]:
            email_address.append(request.form[email_key])            
    try:
        if file:
            filename = secure_filename(file.filename)
            s3.upload_fileobj(file, AWS_Bucket_Name, filename)
            topicARN = create_sns_topic()
            download_link = s3.generate_presigned_url('get_object', Params={'Bucket': AWS_Bucket_Name, 'Key': filename}, ExpiresIn=7000)
            # print(download_link)
            for email in email_address:
                sns.subscribe(TopicArn=topicARN, Protocol='email', Endpoint=email)
                sns.publish(TopicArn=topicARN, Message=download_link, Subject="File Download Link")
            connection = pymysql.connect(host=database_endpoint, user=database_user, password=database_password, database=database_name)
            db_cursor = connection.cursor()
            db_cursor.execute("USE defaultdb;")
            db_cursor.execute("insert into lokifileupload values(%s, %s);", (session['email'], filename))
            connection.commit()
            return render_template('fileupload.html', message="File uploaded & Sent Successfully")
    except Exception as e:
        print("Exception occurred", e)
        return render_template('fileupload.html', message="File upload failed")
    return render_template('fileupload.html')
    
@app.route('/signup', methods=['GET','POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form['confirm_password']

        if password != confirm_password:
            warning = "Password and Confirm Password should be same"
            return render_template('signup.html', warning=warning)

        # check if the email already exists in the database
        if check_user(email):
            warning = "Email already exists! Use another email.."
            return render_template('signup.html', warning=warning)
        try:
            connection = pymysql.connect(host=database_endpoint, user=database_user, password=database_password, database=database_name)
            db_cursor = connection.cursor()
            db_cursor.execute("USE defaultdb;")
            db_cursor.execute("insert into lokiuserrdetails values ('%s', '%s', '%s');" % (username, email, password))
            connection.commit()
            print("User created successfully")
        except Exception as e:
            print("Exception occurred", e)
    return render_template('signup.html')


if __name__ == '__main__':
    create_database()
    app.run(debug=False, host='0.0.0.0')