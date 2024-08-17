from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta
import json
import psycopg
import requests
from contextlib import contextmanager


PG_CONN_ID='PG_WAREHOUSE_CONNECTION'
LIMIT_ROWS=50
KEY_SETTINGS='loaded_rows'

HEADERS={
    'X-Nickname': 'shav_sma_626',
    'X-Cohort': '27',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}

@contextmanager
def PgConnection(id: str):
    conn = BaseHook.get_connection(id)
    kwargs = {
        'host': conn.host,
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password,
        'port': conn.port
    }
    if 'sslmode' in conn.extra_dejson: kwargs['sslmode'] = conn.extra_dejson['sslmode']
    conn = psycopg.connect(**kwargs)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_wf_settings(workflow_key: str) -> int:
    with PgConnection(PG_CONN_ID) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT 
                            workflow_settings->>%(key_settings)s
                        FROM
                            stg.srv_wf_settings
                        WHERE 
                            workflow_key = %(workflow_key)s
                        """, {'workflow_key': workflow_key, 'key_settings': KEY_SETTINGS})
            result = cur.fetchone()
            return int(result[0]) if result else 0

def update_couriers() -> None:
    workflow_key='stg.yandexcloud_couriers'
    loaded_rows=get_wf_settings(workflow_key)
    print(f'Уже загружено строк: {loaded_rows}')
    url=f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=id&sort_direction=asc&limit={LIMIT_ROWS}&offset={loaded_rows}'
    couriers=requests.get(url, headers=HEADERS).json()
    print(f'Необходимо загрузить строк: {len(couriers)}')
    if couriers:
        with PgConnection(PG_CONN_ID) as conn:
            with conn.cursor() as cur:
                for courier in couriers:
                    cur.execute("""
                                INSERT INTO stg.yandexcloud_couriers(object_id, object_value, update_ts)
                                VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                                """, {'object_id': courier['_id'], 'object_value': json.dumps(courier), 'update_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                cur.execute("""
                            INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                            VALUES (%(workflow_key)s, %(workflow_settings)s)
                            ON CONFLICT(workflow_key) DO UPDATE
                            SET workflow_settings=EXCLUDED.workflow_settings
                            """, {'workflow_key': workflow_key, 'workflow_settings': json.dumps({KEY_SETTINGS: loaded_rows+len(couriers)})}
                            )
                print('Новые данные загружены')

def update_deliveries() -> None:
    workflow_key='stg.yandexcloud_deliveries'
    loaded_rows=get_wf_settings(workflow_key)
    print(f'Уже загружено строк: {loaded_rows}')
    from_ts = (datetime.now() - timedelta(7)).strftime('%Y-%m-%d %H:%M:%S')
    to_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    url=f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=id&sort_direction=asc&limit={LIMIT_ROWS}&offset={loaded_rows}&from={from_ts}&to={to_ts}'
    deliveries=requests.get(url, headers=HEADERS).json()
    print(f'Необходимо загрузить строк: {len(deliveries)}')
    if deliveries:
        with PgConnection(PG_CONN_ID) as conn:
            with conn.cursor() as cur:
                for delivery in deliveries:
                    cur.execute("""
                                INSERT INTO stg.yandexcloud_deliveries(object_id, object_value, update_ts)
                                VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                                """, {'object_id': delivery['order_id'], 'object_value': json.dumps(delivery), 'update_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                cur.execute("""
                            INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                            VALUES (%(workflow_key)s, %(workflow_settings)s)
                            ON CONFLICT(workflow_key) DO UPDATE
                            SET workflow_settings=EXCLUDED.workflow_settings
                            """, {'workflow_key': workflow_key, 'workflow_settings': json.dumps({KEY_SETTINGS: loaded_rows+len(deliveries)})}
                            )
                print('Новые данные загружены')

dag=DAG(
    dag_id='de_project_sprint_5_stg',
    schedule_interval='0/15 * * * *',
    start_date=datetime(2024, 8, 1),
    catchup=False
)
update_couriers_task=PythonOperator(
    task_id='update_couriers',
    python_callable=update_couriers,
    dag=dag
)
update_deliveries_task=PythonOperator(
    task_id='update_deliveries',
    python_callable=update_deliveries,
    dag=dag
)

(update_couriers_task, update_deliveries_task)