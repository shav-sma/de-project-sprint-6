from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

PG_CONN_ID='PG_WAREHOUSE_CONNECTION'


dag=DAG(
    dag_id='de_project_sprint_5_cmd',
    schedule_interval='0/15 * * * *',
    start_date=datetime(2024, 8, 1),
    catchup=False
)

update_dm_courier_ledger_task = PostgresOperator(
    task_id="update_dm_courier_ledger",
    postgres_conn_id=PG_CONN_ID,
    sql="""
        DELETE FROM cdm.dm_courier_ledger;
        WITH tmp AS (
            SELECT 
                do2.courier_id,
                dc.courier_name,
                dt."year",
                dt."month",
                COUNT(DISTINCT fd.order_id) orders_count,
                SUM(fd."sum") orders_total_sum,
                AVG(fd.rate) rate_avg,
                SUM(fd."sum") * 0.25 order_processing_fee,
                CASE 
                    WHEN AVG(fd.rate) < 4 THEN GREATEST(SUM(fd."sum") * 0.05, 100.0)
                    WHEN AVG(fd.rate) >= 4 AND AVG(fd.rate) < 4.5 THEN GREATEST(SUM(fd."sum") * 0.07, 150.0)
                    WHEN AVG(fd.rate) >= 4.5 AND AVG(fd.rate) < 4.9 THEN GREATEST(SUM(fd."sum") * 0.08, 175.0)
                    WHEN AVG(fd.rate) >= 4.9 THEN GREATEST(SUM(fd."sum") * 0.1, 200.0)
                END courier_order_sum,
                SUM(fd.tip_sum) courier_tips_sum
            FROM 
                dds.fct_deliveries fd
                INNER JOIN dds.dm_orders do2 ON fd.order_id = do2.id
                INNER JOIN dds.dm_couriers dc ON do2.courier_id = dc.id
                INNER JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
            GROUP BY
                do2.courier_id,
                dc.courier_name,
                dt."year",
                dt."month"
        )

        INSERT INTO cdm.dm_courier_ledger(	courier_id, courier_name, settlement_year, settlement_month,
                                            orders_count, orders_total_sum, rate_avg,
                                            order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
        SELECT *, courier_order_sum + courier_tips_sum * 0.95 courier_reward_sum
        FROM tmp
    """,
    dag=dag
)

update_dm_courier_ledger_task