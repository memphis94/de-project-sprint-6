from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum
import vertica_python

#dotenv
from dotenv import load_dotenv
import os

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv(key='AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv(key='AWS_ACCESS_KEY_ID')

conn_info = {'host': os.getenv(key='VERTICA_HOST'),
             'port': os.getenv(key='VERTICA_PORT'),
             'user': os.getenv(key='VERTICA_USER'),
             'password': os.getenv(key='VERTICA_PASSWORD'),
             'database': os.getenv(key='VERTICA_DATABASE'),             
            'autocommit': True
}

def load_data(key: str, rows: str):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f""" COPY REDRUM94YANDEXRU__STAGING.{key}({rows}) 
                        FROM LOCAL '/data/{key}.csv'
                        DELIMITER ',' enclosed '"'  REJECTED DATA AS TABLE {key}_rej ;""")
        cur.close()
        conn.close()



def fetch_s3_file(bucket: str, key: str):    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

@dag(schedule_interval=None, start_date=pendulum.parse('2022-01-01'))
def sprint6_dag():

    start_task = EmptyOperator(task_id='start')

    end_task = EmptyOperator(task_id='end')

    bucket_files = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']

    task1 = PythonOperator(
        task_id='fetch_groups_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )

    task2 = PythonOperator(
        task_id='fetch_users_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )

    task3 = PythonOperator(
        task_id='fetch_dialogs_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )

    task4 = PythonOperator(
        task_id='fetch_group_log_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command="for file in /data/*.csv; do echo $file && head -n 10 $file; done",
        params={'files': [f'/data/{f}' for f in bucket_files]}
    )

    load_data_users = PythonOperator(
        task_id='load_data_users',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'users', 'rows': 'id,chat_name,registration_dt,country,age'}
    )

    load_data_groups = PythonOperator(
        task_id='load_data_groups',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups', 'rows': 'id,admin_id,group_name,registration_dt,is_private'}
    )
    load_data_dialogs = PythonOperator(
        task_id='load_data_dialogs',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs', 'rows': 'message_id,message_ts,message_from,message_to,message,message_type'}
    )
    load_data_group_log = PythonOperator(
        task_id='load_data_group_log',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log', 'rows': 'group_id,user_id,user_id_from,event,datetime'}
    )

    
    start_task >> [task1, task2, task3, task4] >> print_10_lines_of_each >> [load_data_users, load_data_groups, load_data_dialogs, load_data_group_log] >> end_task


sprint6_dag = sprint6_dag()