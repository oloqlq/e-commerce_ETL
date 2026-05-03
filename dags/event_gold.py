'''
    -- gold_search (검색 키워드 빈도): action=search, search_keyword별 그룹화
       -> 날짜별? / 전체 집계?
    -- gold_funnel (퍼널)
    -- gold_device (디바이스별)
'''

# bronze raw -> s3://버킷/bronze/year=2026/month=05...
# Athena가 S3에 파티션 폴더 생겨도 자동 인식 못함 -> 파티션 등록
# silver S3://버킷/silver/event/year=2026/month=05..
# 이런 식으로 해야되는지
# 그렇게 바꾸면 테이블 생성, 데이터 삽입할 때 뭐 수정해야 하는지

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd

DATABASE_SILVER = 'ecommerce_silver_db'
DATABASE_GOLD   = 'ecommerce_gold_db'
GOLD_S3_PATH    = 's3://de-ai-14-827913617635-ap-northeast-1-an/gold/'
ATHENA_RESULTS  = 's3://de-ai-14-827913617635-ap-northeast-1-an/athena-results/'
BUCKET = 'de-ai-14-827913617635-ap-northeast-1-an'

def cleanup_gold_partition(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3   = hook.get_conn()
    
    prefixes = [
        f"gold/funnel/event_date={target_dt}/",
        f"gold/device/event_date={target_dt}/",
        f"gold/search/event_date={target_dt}/"
    ]

    for prefix in prefixes:
        paginator = s3.get_paginator("list_objects_v2")
        batch = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})
        if batch:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch})
            print(f"삭제 완료: {len(batch)}개 파일")


with DAG(
    dag_id="silver_to_gold_event",
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "event"],
) as dag:
    
    wait_for_event_silver = ExternalTaskSensor(
        task_id="wait_for_event_silver",
        external_dag_id="bronze_to_silver_event",
        external_task_id=None,
        allowed_states=["success"],
        execution_delta=None,
        execution_date_fn=lambda dt: dt,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    wait_for_sales_silver = ExternalTaskSensor(
        task_id="wait_for_sales_silver",
        external_dag_id="bronze_to_silver_sales",
        external_task_id=None,
        allowed_states=["success"],
        execution_delta=None,
        execution_date_fn=lambda dt: dt,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    cleanup_gold = PythonOperator(
        task_id = "cleanup_gold",
        python_callable= cleanup_gold_partition,
        op_kwargs= {"target_dt": "{{ macros.ds_add(ds, -1) }}"}
    )
    
    # 액션별 세션 수
    create_gold_funnel = AthenaOperator(
        task_id = "create_gold_funnel",
        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_funnel (
                action          STRING,
                session_count   BIGINT
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}funnel/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    create_gold_device = AthenaOperator(
        task_id = "create_gold_device",
        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_device (
                device  STRING,
                action  STRING,
                cnt     BIGINT
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}device/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    create_gold_search = AthenaOperator(
        task_id = "create_gold_search",
        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_search (
                search_keyword          STRING,
                count_search_keyword    BIGINT
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}search/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    create_gold_campaign = AthenaOperator(
        task_id = "create_gold_campaign",
        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_campaign (
                campaign_id         STRING,
                visit_user_count    BIGINT,
                purchase_count      BIGINT,
                conversion_rate     DOUBLE
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}campaign/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params = {
            "database_gold":   DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    create_gold_item = AthenaOperator(
        task_id = "create_gold_item",
        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_item (
                item_id          STRING,
                view_count       BIGINT,
                cart_count       BIGINT,
                purchase_count   BIGINT,
                conversion_rate  DOUBLE,
                total_revenue    BIGINT,
                avg_order_amount DOUBLE
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}item/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params = {
            "database_gold":   DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    insert_gold_funnel = AthenaOperator(
        task_id = "insert_gold_funnel",
        query = """
            INSERT INTO {{ params.database_gold }}.gold_funnel
            SELECT
                action,
                COUNT(DISTINCT session_id) AS session_count,
                event_date
            FROM {{ params.database_silver }}.silver_event
            WHERE event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
            GROUP BY action, event_date;            
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )
    
    insert_gold_device = AthenaOperator(
        task_id = "insert_gold_device",
        query = """
            INSERT INTO {{ params.database_gold }}.gold_device
            SELECT
                device,
                action,
                COUNT(*) AS cnt,
                event_date
            FROM {{ params.database_silver }}.silver_event
            WHERE event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
            GROUP BY device, action, event_date;
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )
    
    insert_gold_search = AthenaOperator(
        task_id = "insert_gold_search",
        query = """
            INSERT INTO {{ params.database_gold }}.gold_search
            SELECT
                search_keyword,
                COUNT(*) AS count_search_keyword,
                event_date
            FROM {{ params.database_silver }}.silver_event
            WHERE event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
            AND action = 'search'
            GROUP BY search_keyword, event_date
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )

    insert_gold_campaign = AthenaOperator(
        task_id = "insert_gold_campaign",
        query = """
            INSERT INTO {{ params.database_gold }}.gold_campaign
            SELECT
                campaign_id,
                COUNT(DISTINCT user_id)                                     AS visit_user_count,
                COUNT(DISTINCT CASE WHEN action = 'purchase' THEN session_id END) AS purchase_count,
                ROUND(
                    COUNT(DISTINCT CASE WHEN action = 'purchase' THEN session_id END) * 100.0
                    / NULLIF(COUNT(DISTINCT session_id), 0), 2
                )                                                           AS conversion_rate,
                event_date
            FROM {{ params.database_silver }}.silver_event
            WHERE event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
            AND campaign_id IS NOT NULL
            GROUP BY campaign_id, event_date;
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )

    insert_gold_item = AthenaOperator(
        task_id = "insert_gold_item",
        query = """
            INSERT INTO {{ params.database_gold }}.gold_item
            SELECT
                e.item_id,
                COUNT(DISTINCT CASE WHEN e.action = 'view'        THEN e.session_id END) AS view_count,
                COUNT(DISTINCT CASE WHEN e.action = 'add_to_cart' THEN e.session_id END) AS cart_count,
                COUNT(DISTINCT CASE WHEN e.action = 'purchase'    THEN e.session_id END) AS purchase_count,
                ROUND(
                    COUNT(DISTINCT CASE WHEN e.action = 'purchase' THEN e.session_id END) * 100.0
                    / NULLIF(COUNT(DISTINCT CASE WHEN e.action = 'view' THEN e.session_id END), 0), 2
                )                                                                         AS conversion_rate,
                COALESCE(SUM(s.total_amount), 0)                                          AS total_revenue,
                ROUND(COALESCE(AVG(s.total_amount), 0), 2)                                AS avg_order_amount,
                e.event_date
            FROM {{ params.database_silver }}.silver_event e
            LEFT JOIN {{ params.database_silver }}.silver_sales s
                ON e.event_id = s.order_id
            WHERE e.event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
            AND e.item_id IS NOT NULL
            GROUP BY e.item_id, e.event_date;
        """,
        params={
            "database_gold":   DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )

    [wait_for_event_silver, wait_for_sales_silver] >> cleanup_gold >> [
        create_gold_funnel,
        create_gold_device,
        create_gold_search,
        create_gold_campaign,
        create_gold_item
    ]

    create_gold_funnel >> insert_gold_funnel
    create_gold_device >> insert_gold_device
    create_gold_search >> insert_gold_search
    create_gold_campaign >> insert_gold_campaign
    create_gold_item >> insert_gold_item