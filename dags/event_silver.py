from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import pandas as pd

'''
    Airflow에서 AWS 연결 설정
    Admin → Connections → Add
    Conn Id   : aws_default
    Conn Type : Amazon Web Services
    Login     : ACCESS_KEY
    Password  : SECRET_KEY
    Extra     : {"region_name": "ap-northeast-1"}
'''

# Athena ecommerce_bronze_db -> bronze_event
DATABASE_BRONZE = 'ecommerce_bronze_db'
DATABASE_SILVER = 'ecommerce_silver_db'
BUCKET = 'de-ai-14-827913617635-ap-northeast-1-an'
SILVER_S3_PATH = 's3://de-ai-14-827913617635-ap-northeast-1-an/silver/event/'
ATHENA_RESULTS = 's3://de-ai-14-827913617635-ap-northeast-1-an/athena-results/'
SILVER_TBL_NAME = 'silver_event'

def cleanup_silver_partition(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3 = hook.get_conn()
    
    prefix = f"silver/event/event_date={target_dt}"

    paginator = s3.get_paginator("list_objects_v2")
    batch = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})

    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch})
        print(f"삭제 완료: {len(batch)}개 파일")
    else:
        print(f"삭제할 파일 없음: {prefix}")


with DAG(
    dag_id="bronze_to_silver_event",
    description= "event silver 테이블 구성 및 데이터 증분 작업",
    default_args={
        "owner":       "airflow",             # DAG 소유자 (Airflow UI에 표시)
        "retries":     1,                     # 실패시 재시도 횟수
        "retry_delay": timedelta(minutes=5),  # 재시도 간격
    },
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 1, 1),          # 언제부터 실행될 수 있는지
    catchup=False,                            # 밀린 날짜 실행할지
    tags=["silver", "event"],
) as dag:
    # t1: 멱등성 보장, DAG 수동으로 여러번 실행시
    #     S3 silver/event/event_date=''/ 파일이 여러개 생성될 수 있음
    cleanup_task = PythonOperator(
        task_id = 'cleanup_silver_partition',
        python_callable = cleanup_silver_partition,
        op_kwargs = {"target_dt": "{{ macros.ds_add(ds, -1) }}"}
    )

    # t2: silver Table 없을 경우에 생성 (구조만)
    create_silver_table = AthenaOperator(
        task_id = 'create_silver_table_if_not_exists',
        query= """
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_silver }}.{{ params.tbl_name }} (
                event_id        STRING,
                event_timestamp TIMESTAMP,
                event_hour      INT,
                event_dow       STRING,
                user_id         STRING,
                is_member       BOOLEAN,
                session_id      STRING,
                item_id         STRING,
                action          STRING,
                page_type       STRING,
                device          STRING,
                platform        STRING,
                referrer        STRING,
                campaign_id     STRING,
                search_keyword  STRING,
                is_valid_action BOOLEAN
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.silver_path }}'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params= {
            'database_silver': DATABASE_SILVER,
            'silver_path': SILVER_S3_PATH,
            'tbl_name': SILVER_TBL_NAME
        },
        database= DATABASE_SILVER,
        output_location= ATHENA_RESULTS
    )

    
    # t3: 특정 시간대 데이터 추출해서 silver 테이블에 삽입 (execution_date 활용)
    insert_silver = AthenaOperator(
        task_id="insert_silver",
        query="""
            INSERT INTO {{ params.database_silver }}.{{ params.tbl_name}}
            SELECT
                event_id,

                CAST(event_timestamp AS TIMESTAMP)          AS event_timestamp,
                CAST(HOUR(CAST(event_timestamp AS TIMESTAMP)) AS INT)    AS event_hour,
                DATE_FORMAT(
                    CAST(event_timestamp AS TIMESTAMP),
                    '%a'
                )                                           AS event_dow,

                user_id,
                CASE WHEN user_id IS NOT NULL
                     THEN TRUE ELSE FALSE END               AS is_member,
                session_id,

                item_id,
                action,
                page_type,

                device,
                platform,
                referrer,
                campaign_id,
                search_keyword,

                CASE WHEN action IN (
                    'view','click','add_to_cart','wishlist','search','purchase'
                ) THEN TRUE ELSE FALSE END                  AS is_valid_action,

                CAST(
                    CAST(event_timestamp AS TIMESTAMP) AS DATE
                )                                           AS event_date

            FROM {{ params.database_bronze }}.bronze_event

            WHERE CAST(
                CAST(event_timestamp AS TIMESTAMP) AS DATE
            ) = DATE('{{ ds }}') - INTERVAL '1' DAY

            AND action IN (
                'view','click','add_to_cart','wishlist','search','purchase'
            );
        """,
        params= {
            'database_bronze': DATABASE_BRONZE,
            'database_silver': DATABASE_SILVER,
            'silver_path': SILVER_S3_PATH,
            'tbl_name': SILVER_TBL_NAME
        },
        database= DATABASE_SILVER,
        output_location=ATHENA_RESULTS
    )

    cleanup_task >> create_silver_table >> insert_silver