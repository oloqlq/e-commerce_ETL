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
    
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="bronze_to_silver_event",
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
    
    # 이건 뭘 분석하는거야
    create_gold_funnel = AthenaOperator(
        task_id = "create_gold_funnel",
        query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_GOLD}.gold_funnel (
                action          STRING,
                session_count   BIGINT
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{GOLD_S3_PATH}funnel/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    create_gold_device = AthenaOperator(
        task_id = "create_gold_device",
        query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_GOLD}.gold_device (
                device  STRING,
                action  STRING,
                cnt     BIGINT
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{GOLD_S3_PATH}device/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    create_gold_search = AthenaOperator(
        task_id = "create_gold_search",
        query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_GOLD}.gold_search (
                search_keyword          STRING,
                count_search_keyword    BIGINT
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{GOLD_S3_PATH}search/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS
    )

    insert_gold_funnel = AthenaOperator(
        task_id = "insert_gold_funnel",
        query = f"""
            INSERT INTO {DATABASE_GOLD}.gold_funnel
            SELECT
                action,
                COUNT(DISTINCT session_id) AS session_count,
                event_date
            FROM {DATABASE_SILVER}.silver_event
            WHERE event_date = DATE('{{{{ ds }}}}') - INTERVAL '1' DAY
            GROUP BY action, event_date;            
        """,
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )
    
    insert_gold_device = AthenaOperator(
        task_id = "insert_gold_device",
        query = f"""
            INSERT INTO {DATABASE_GOLD}.gold_device
            SELECT
                device,
                action,
                COUNT(*) AS cnt,
                event_date
            FROM {DATABASE_SILVER}.silver_event
            WHERE event_date = DATE('{{{{ ds }}}}') - INTERVAL '1' DAY
            GROUP BY device, action, event_date;
        """,
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )
    
    insert_gold_search = AthenaOperator(
        task_id = "insert_gold_search",
        query = f"""
            INSERT INTO {DATABASE_GOLD}.gold_search
            SELECT
                search_keyword,
                COUNT(*) AS count_search_keyword,
                event_date
            FROM {DATABASE_SILVER}.silver_event
            WHERE event_date = DATE('{{{{ ds }}}}') - INTERVAL '1' DAY
            AND action = 'search'
            GROUP BY search_keyword, event_date
        """,
        database = DATABASE_GOLD,
        output_location = ATHENA_RESULTS,
    )

    wait_for_silver >> cleanup_gold >> [
        create_gold_funnel,
        create_gold_device,
        create_gold_search
    ]

    create_gold_funnel >> insert_gold_funnel
    create_gold_device >> insert_gold_device
    create_gold_search >> insert_gold_search