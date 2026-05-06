#######################################
# import, config
#######################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.email import send_email
import logging
import requests
from datetime import datetime, timedelta
import os
import great_expectations as ge
import pandas as pd
import io

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

logger = logging.getLogger(__name__)



#######################################
# call_back functions
#######################################

def check_bronze_data(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3 = hook.get_conn()

    response = s3.list_objects_v2(
        Bucket = BUCKET,
        Prefix = f"raw/{target_dt[:4]}/{target_dt[5:7]}/{target_dt[8:10]}/"
    )

    if response.get("KeyCount", 0) == 0:
        raise ValueError(f"브론즈 데이터 없음: {target_dt}")
    
    print(f"브론즈 데이터 확인: {response['KeyCount']}개 파일")

def cleanup_silver_partition(target_dt, **kwargs):
    logger.info(f"[START] cleanup_silver_partition | target_dt={target_dt}")

    hook = S3Hook(aws_conn_id="aws_default")
    s3 = hook.get_conn()
    
    prefix = f"silver/event/event_date={target_dt}"
    logger.info(f"S3 prefix: {prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    batch = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})

    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch})
        logger.info(f"[DELETE] {len(batch)} files removed")
    else:
        logger.warning(f"[SKIP] No files to delete: {prefix}")


    logger.info(f"[END] cleanup_silver_partition")

def alert_email(context):
    subject = f"[Airflow] Task Faild: {context['task_instance'].task_id}"
    body = f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution Time: {context['execution_date']}
        Log: {context['task_instance'].log_url}
    """
    send_email(to=[os.getenv("ALERT_EMAIL")], subject=subject, html_content=body)

def alert_slack(context):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    msg = f"""
        Task Faild
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Time: {context['execution_date']}
        Log: {context['task_instance'].log_url}
    """

    logger.error(f"[ALERT] Sending Slack alert for {context['task_instance'].task_id}")

    requests.post(webhook_url, json={"text": msg})

def alert_all(context):
    logger.error(f"[ALERT] Triggered for DAG={context['dag'].dag_id}")
    alert_email(context)
    alert_slack(context)


'''
데이터 품질 검증 기능 추가
'''
def validate_event_silver(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3 = hook.get_conn()

    prefix = f"silver/event/event_date={target_dt}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    parquet_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_keys:
        raise ValueError(f"검증 대상 silver/event 데이터 없음: {target_dt}")

    dfs = []
    for key in parquet_keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        dfs.append(pd.read_parquet(io.BytesIO(body), engine="pyarrow"))

    df = pd.concat(dfs, ignore_index=True)

    ge_df = ge.from_pandas(df)

    ge_df.expect_column_values_to_not_be_null("event_id")
    ge_df.expect_column_values_to_be_in_set(
        "action",
        ["view", "click", "add_to_cart", "wishlist", "search", "purchase"]
    )
    ge_df.expect_column_values_to_be_between("event_hour", 0, 23)

    results = ge_df.validate()

    if not results["success"]:
        raise ValueError(f"event silver 데이터 품질 검증 실패: {target_dt}")

    print(f"[검증 완료] event silver 데이터 품질 이상 없음: {target_dt}")


#######################################
# DAG 
#######################################

with DAG(
    dag_id="bronze_to_silver_event",
    description= "event silver 테이블 구성 및 데이터 증분 작업",
    default_args={
        "owner":       "airflow",             # DAG 소유자 (Airflow UI에 표시)
        "retries":     0,                     # 실패시 재시도 횟수
        "retry_delay": timedelta(minutes=5),  # 재시도 간격
        "on_failure_callback": alert_all,
    },
    schedule_interval="10 0 * * *",
    start_date=datetime(2026, 1, 1),          # 언제부터 실행될 수 있는지
    catchup=False,                            # 밀린 날짜 실행할지
    tags=["silver", "event"],
) as dag:
    
    # t1: 브론즈 데이터 확인
    check_bronze = PythonOperator(
        task_id = "check_bronze_data",
        python_callable=check_bronze_data,
        op_kwargs={"target_dt": "{{ macros.ds_add(ds, -1) }}"}
    )

    # t2: 멱등성 보장, DAG 수동으로 여러번 실행시
    #     S3 silver/event/event_date=''/ 파일이 여러개 생성될 수 있음
    cleanup_task = PythonOperator(
        task_id = 'cleanup_silver_partition',
        python_callable = cleanup_silver_partition,
        op_kwargs = {"target_dt": "{{ ds }}"}
    )

    # t3: silver Table 없을 경우에 생성 (구조만)
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

    
    # t4: 특정 시간대 데이터 추출해서 silver 테이블에 삽입 (execution_date 활용)
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
            ) = DATE('{{ ds }}')

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

    # t5 : 데이터 품질 검증 
    validate_task = PythonOperator(
    task_id="validate_event_silver",
    python_callable=validate_event_silver,
    op_kwargs={"target_dt": "{{ macros.ds_add(ds, -1) }}"}
)

    check_bronze >> cleanup_task >> create_silver_table >> insert_silver >> validate_task