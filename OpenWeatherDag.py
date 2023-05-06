import requests
import json
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import pendulum


# timezone 한국시간으로 변경
local_tz = pendulum.timezone("Asia/Seoul")

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    
    # Variables 활용
    api_key = Variable.get("open_weather_api_key")
    lat = 37.5665
    lon = 126.9780
    
    # https://openweathermap.org/api/one-call-api
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
    response = requests.get(url)
    data = json.loads(response.text)
    logging.info('############ extract success')
    return data


def transform_load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    sql = "BEGIN; DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    for d in data['daily']:
        if d != "":
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{datetime.fromtimestamp(d['dt']).strftime('%Y-%m-%d')}', '{d['temp']['day']}', '{d['temp']['min']}', '{d['temp']['max']}');"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)
    logging.info('############ transform_load success')


#######################################################################

weather_dag = DAG(
    dag_id = 'OpenWeatherDag',
    start_date = datetime(2023,4,6, tzinfo = local_tz), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 6 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    # default_args = {
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=1),
    #     # 'on_failure_callback': slack.on_failure_callback,
    # }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    dag = weather_dag)


transform_load = PythonOperator(
    task_id = 'transform_load',
    python_callable = transform_load,
    params = {
        'schema': 'tkddnr961224',   ## 자신의 스키마로 변경
        'table': 'weather_forecast'
    },
    dag = weather_dag)

extract >> transform_load


















# CREATE TABLE keeyong.weather_forecast (
#  date date primary key,
#  temp float, -- 낮 온도
#  min_temp float,
#  max_temp float,
#  created_date timestamp default GETDATE()
# );
