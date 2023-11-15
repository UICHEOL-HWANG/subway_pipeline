from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
import pymysql
import pandas as pd
import pendulum
import pymysql 
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb



# AWS 및 S3 연결 정보
AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'key'
AWS_REGION = 'ap-northeast-2'  # 예: 'us-west-1'
BUCKET_NAME = 'bucket'
kst = pendulum.timezone("Asia/Seoul") #한국 시간으로 변경


# MySQL 연결 정보
MYSQL_CONN_ID = 'airflow_db'

dag = DAG(
    'seoul_subway_api_pipeline',
    description='DAG to fetch Seoul subway API data, upload to S3, and load into MySQL',
    schedule_interval='10 21 * * *',  # 매일 UTC 20시에 실행
    start_date=datetime(2023, 11, 14, 20, 0, 0, tzinfo=kst),  # 한국 시간 기준으로 2023-11-14 20:00:00에 시작
    catchup=False,  # 과거 실행은 무시
)


def get_three_days_ago_endpoint(api_key):
    three_days_ago = datetime.now() - timedelta(days=3)
    formatted_date = three_days_ago.strftime("%Y%m%d")
    base_url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayStatsNew/1/1000/{formatted_date}"
    response = requests.get(base_url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error fetching data for date: {formatted_date}")
        return None

def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    api_key = 'key'
    data = get_three_days_ago_endpoint(api_key)

    if data:
        json_data = json.dumps(data)
        s3_hook = S3Hook(aws_conn_id='aws_default')
        formatted_date = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
        object_key = f'subway/subway_{formatted_date}.json'
        s3_hook.load_string(
            string_data=json_data,
            key=object_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f'File uploaded successfully to s3://{BUCKET_NAME}/{object_key}')
        return object_key
    else:
        print(f"No data found for date: {formatted_date}")
        return None

def load_to_mysql(**kwargs):
    ti = kwargs['ti']
    object_key = ti.xcom_pull(task_ids='upload_to_s3_task')

    if object_key:
        s3_hook = S3Hook(aws_conn_id='aws_default')
        response = s3_hook.read_key(object_key, bucket_name=BUCKET_NAME)

        # JSON 데이터를 데이터프레임으로 변환
        json_data = json.loads(response)

        df_s3 = pd.DataFrame(json_data["CardSubwayStatsNew"]["row"])

  	# SQLAlchemy를 사용하여 MySQL 연결
        engine = create_engine("mysql+mysqldb://id:password@ip/db?charset=utf8mb4")
        conn = engine.connect()

        # MySQL에서 기존 데이터를 데이터프레임으로 가져옴
        query = 'SELECT * FROM seoul'
        df_mysql = pd.read_sql_query(query, con=conn)

        # 두 데이터프레임을 병합
        df_concatenated = pd.concat([df_mysql, df_s3])

        # MySQL에 새로운 데이터프레임으로 덮어쓰기 (if_exists='replace')
        df_concatenated.to_sql(name='seoul', con=conn, if_exists='replace', index=False)

         #연결 닫기
        conn.close()

        print(f'Data loaded successfully into MySQL from s3://{BUCKET_NAME}/{object_key}')
    else:
        print('No data available for MySQL loading')


# task 생성
upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

load_to_mysql_task = PythonOperator(
    task_id='load_to_mysql_task',
    python_callable=load_to_mysql,
    provide_context=True,
    dag=dag,
)

# task 간의 의존성 설정
upload_to_s3_task >> load_to_mysql_task
