from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import pendulum

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

bash_command_tmpl = """
head {{ params.files }}
"""

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
    return f'/data/{key}'  # Return the downloaded file path

@dag(schedule_interval=None, start_date=pendulum.parse('2023-01-13'))
def project6_dag_get_data():
    bucket_files = ['group_log.csv']  # Use a list if you intend to fetch multiple files

    fetch_tasks = []
    for key in bucket_files:
        fetch_task = PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        )
        fetch_tasks.append(fetch_task)

    print_10_lines = BashOperator(
        task_id='print_10_lines',
        bash_command=bash_command_tmpl,  # Use the template you defined earlier
        params={'files': " ".join([f'/data/{f}' for f in bucket_files])}  # Provide the template context
    )

    fetch_tasks >> print_10_lines

project6_dag = project6_dag_get_data()
