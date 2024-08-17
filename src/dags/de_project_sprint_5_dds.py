from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

PG_CONN_ID='PG_WAREHOUSE_CONNECTION'


dag=DAG(
    dag_id='de_project_sprint_5_dds',
    schedule_interval='0/15 * * * *',
    start_date=datetime(2024, 8, 1),
    catchup=False
)

update_dm_couriers_task = PostgresOperator(
    task_id="update_dm_couriers",
    postgres_conn_id=PG_CONN_ID,
    sql="""
        WITH
            key_settings AS (
                SELECT 
                    COALESCE((	SELECT (workflow_settings->>'last_loaded_ts')::TIMESTAMP
                                FROM dds.srv_wf_settings
                                WHERE workflow_key = 'dds.dm_couriers'),
                            '2020-01-01 00:00:00.000'::TIMESTAMP) "key"
            ),
            src AS (
                SELECT
                    object_value::JSON->>'_id' id,
                    object_value::JSON->>'name' "name",
                    update_ts
                FROM 
                    stg.yandexcloud_couriers
                WHERE 
                    update_ts > (SELECT * FROM key_settings)
            ),
            update_dm_couriers AS (
                INSERT INTO dds.dm_couriers (courier_id, courier_name)
                SELECT id, "name" FROM src
            ),
            update_srv_wf_settings AS (
                INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                SELECT 
                    'dds.dm_couriers'::VARCHAR,
                    CAST('{"last_loaded_ts": "' || COALESCE((SELECT MAX(update_ts) FROM src), (SELECT "key" FROM key_settings)) || '"}' AS JSON)
                ON CONFLICT(workflow_key) DO UPDATE
                SET workflow_settings=EXCLUDED.workflow_settings
            ) 
        SELECT current_timestamp 
    """,
    dag=dag
)

update_dm_orders_task = PostgresOperator(
    task_id="update_dm_orders",
    postgres_conn_id=PG_CONN_ID,
    sql="""
        WITH
            key_settings AS (
                SELECT 
                    COALESCE((	SELECT (workflow_settings->>'last_loaded_ts')::TIMESTAMP
                                FROM dds.srv_wf_settings
                                WHERE workflow_key = 'dds.dm_orders'),
                            '2020-01-01 00:00:00.000'::TIMESTAMP) "key"
            ),
            src AS (
                SELECT
                    oo.object_id order_id,
                    oo.object_value::JSON->>'final_status' order_status,
                    dr.id restaurant_id,
                    dt.id timestamp_id,
                    du.id user_id,
                    dc.id courier_id,
                    oo.update_ts
                FROM 
                    stg.ordersystem_orders oo
                    INNER JOIN stg.yandexcloud_deliveries yd ON oo.object_id = yd.object_id
                    INNER JOIN dds.dm_restaurants dr ON dr.restaurant_id = (oo.object_value::JSON->>'restaurant')::JSON->>'id'
                    INNER JOIN dds.dm_timestamps dt ON dt.ts = (oo.object_value::JSON->>'date')::TIMESTAMP
                    INNER JOIN dds.dm_users du ON du.user_id = (oo.object_value::JSON->>'user')::JSON->>'id'
                    INNER JOIN dds.dm_couriers dc ON dc.courier_id = yd.object_value::JSON->>'courier_id'
                WHERE 
                    oo.update_ts > (SELECT * FROM key_settings)
            ),
            update_dm_orders AS (
                INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id, courier_id)
                SELECT order_id, order_status, restaurant_id, timestamp_id, user_id, courier_id FROM src
            ),
            update_srv_wf_settings AS (
                INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                SELECT 
                    'dds.dm_orders'::VARCHAR,
                    CAST('{"last_loaded_ts": "' || COALESCE((SELECT MAX(update_ts) FROM src), (SELECT "key" FROM key_settings)) || '"}' AS JSON)
                ON CONFLICT(workflow_key) DO UPDATE
                SET workflow_settings=EXCLUDED.workflow_settings
            ) 
        SELECT current_timestamp 
    """,
    dag=dag
)

update_fct_deliveries_task = PostgresOperator(
    task_id="update_fct_deliveries",
    postgres_conn_id=PG_CONN_ID,
    sql="""
        WITH
            key_settings AS (
                SELECT 
                    COALESCE((	SELECT (workflow_settings->>'last_loaded_ts')::TIMESTAMP
                                FROM dds.srv_wf_settings
                                WHERE workflow_key = 'dds.fct_deliveries'),
                            '2020-01-01 00:00:00.000'::TIMESTAMP) "key"
            ),
            src AS (
                SELECT 
                    do2.id order_id,
                    yd.object_value::JSON->>'delivery_id' delivery_id,
                    yd.object_value::JSON->>'address' address,
                    (yd.object_value::JSON->>'delivery_ts')::TIMESTAMP delivery_ts,
                    (yd.object_value::JSON->>'rate')::SMALLINT rate,
                    (yd.object_value::JSON->>'sum')::NUMERIC "sum",
                    (yd.object_value::JSON->>'tip_sum')::NUMERIC tip_sum,
                    yd.update_ts
                FROM 
                    stg.yandexcloud_deliveries yd
                    INNER JOIN dds.dm_orders do2 ON yd.object_id = do2.order_key 
                WHERE 
                    yd.update_ts > (SELECT * FROM key_settings)
            ),
            update_fct_deliveries AS (
                INSERT INTO dds.fct_deliveries (order_id, delivery_id, address, delivery_ts, rate, "sum", tip_sum)
                SELECT order_id, delivery_id, address, delivery_ts, rate, "sum", tip_sum FROM src
            ),
            update_srv_wf_settings AS (
                INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                SELECT 
                    'dds.fct_deliveries'::VARCHAR,
                    CAST('{"last_loaded_ts": "' || COALESCE((SELECT MAX(update_ts) FROM src), (SELECT "key" FROM key_settings)) || '"}' AS JSON)
                ON CONFLICT(workflow_key) DO UPDATE
                SET workflow_settings=EXCLUDED.workflow_settings
            ) 
        SELECT current_timestamp 
    """,
    dag=dag
)


update_dm_couriers_task >> update_dm_orders_task >> update_fct_deliveries_task