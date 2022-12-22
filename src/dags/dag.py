import os
import datetime as dt

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'


default_args = {
    'owner': 'Швейников Андрей',
}
with DAG(
        'events',
        default_args=default_args,
        description='Получение данных о событиях.',
        start_date=dt.datetime(2022, 12, 22),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
) as dag:

    marts = []

    for num in range(1, 4):
        marts.append(
            SparkSubmitOperator(
                task_id=f'mart_{num}',
                application=f'/lessons/dags/mart_{num}.py',
                conn_id='yarn_spark',
                application_args=[
                    'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv',
                    '2022-05-21',
                    30,
                    '/user/master/data/geo/events',
                    f'/user/andreydzr/data/analytics/mart_{num}',
                ],
            )
        )

    marts
