##########################################
# import, config
##########################################

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

DATABASE_BRONZE = 'ecommerce_bronze_db'
DATABASE_SILVER = 'ecommerce_silver_db'
BUCKET          = 'de-ai-14-827913617635-ap-northeast-1-an'
SILVER_S3_PATH  = 's3://de-ai-14-827913617635-ap-northeast-1-an/silver/sales/'
ATHENA_RESULTS  = 's3://de-ai-14-827913617635-ap-northeast-1-an/athena-results/'
SILVER_TBL_NAME = 'silver_sales'

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

# ── cleanup ───────────────────────────────────────
def cleanup_silver_sales_partition(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3   = hook.get_conn()

    prefix = f"silver/sales/dt={target_dt}/"

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
def validate_sales_silver(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3 = hook.get_conn()

    prefix = f"silver/sales/dt={target_dt}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    parquet_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_keys:
        raise ValueError(f"검증 대상 silver/sales 데이터 없음: {target_dt}")

    dfs = []
    for key in parquet_keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        dfs.append(pd.read_parquet(io.BytesIO(body), engine="pyarrow"))

    df = pd.concat(dfs, ignore_index=True)

    ge_df = ge.from_pandas(df)

    ge_df.expect_column_values_to_not_be_null("order_id")
    ge_df.expect_column_values_to_not_be_null("order_time")
    ge_df.expect_column_values_to_not_be_null("item_id")
    ge_df.expect_column_values_to_not_be_null("category")
    ge_df.expect_column_values_to_be_between("unit_price", min_value=1)
    ge_df.expect_column_values_to_be_between("quantity", min_value=1)
    ge_df.expect_column_values_to_be_between("total_amount", min_value=0)

    results = ge_df.validate()

    if not results["success"]:
        raise ValueError(f"sales silver 데이터 품질 검증 실패: {target_dt}")

    print(f"[검증 완료] sales silver 데이터 품질 이상 없음: {target_dt}")


#######################################
# DAG 
#######################################
with DAG(
    dag_id="bronze_to_silver_sales",
    description="sales silver 테이블 구성 및 데이터 증분 작업",
    default_args={
        "owner":       "airflow",
        "retries":     0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_all
    },
    schedule_interval="25 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "sales"],
) as dag:
    # t1: 브론즈 데이터 확인
    check_bronze = PythonOperator(
        task_id = "check_bronze_data",
        python_callable=check_bronze_data,
        op_kwargs={"target_dt": "{{ ds }}"}
    )

    # t2: cleanup
    cleanup_task = PythonOperator(
        task_id="cleanup_silver_sales_partition",
        python_callable=cleanup_silver_sales_partition,
        op_kwargs={"target_dt": "{{ ds }}"},
    )

    # t3: 테이블 없으면 생성
    create_silver_sales = AthenaOperator(
        task_id="create_silver_sales_if_not_exists",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_silver }}.{{ params.tbl_name }} (
                order_id         STRING,
                user_id          STRING,
                order_time       TIMESTAMP,
                region           STRING,
                item_id          STRING,
                category         STRING,
                unit_price       BIGINT,
                quantity         INT,
                discount_amount  INT,
                total_amount     BIGINT,
                payment_method   STRING
            )
            PARTITIONED BY (dt DATE)
            STORED AS PARQUET
            LOCATION '{{ params.silver_path }}'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            'database_silver': DATABASE_SILVER,
            'silver_path':     SILVER_S3_PATH,
            'tbl_name':        SILVER_TBL_NAME,
        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS,
    )

    # t4: 어제 데이터만 INSERT
    insert_silver_sales = AthenaOperator(
        task_id="insert_silver_sales",
        query="""
            INSERT INTO {{ params.database_silver }}.{{ params.tbl_name }}
            SELECT
                s.event_id                                        AS order_id,
                s.user_id,
                CAST(s.event_timestamp AS TIMESTAMP)              AS order_time,
                s.region,
                s.item_id,
                pm.category,
                pm.price                                          AS unit_price,
                s.quantity                                        AS quantity,
                s.discount_amount                                 AS discount_amount,
                (pm.price * s.quantity) - s.discount_amount       AS total_amount,
                payment_method,
                CAST(
                    CAST(s.event_timestamp AS TIMESTAMP) AS DATE
                )                                               AS dt
            FROM {{ params.database_bronze }}.bronze_sales s
            LEFT JOIN {{ params.database_bronze }}.product_master pm
                ON s.item_id = pm.item_id
            WHERE action = 'purchase'
            AND CAST(
                CAST(s.event_timestamp AS TIMESTAMP) AS DATE
            ) = DATE('{{ ds }}');
        """,
        params={
            'database_bronze': DATABASE_BRONZE,
            'database_silver': DATABASE_SILVER,
            'tbl_name':        SILVER_TBL_NAME,
        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS,
    )

    # t5: 데이터 품질 검증
    validate_task = PythonOperator(
    task_id="validate_sales_silver",
    python_callable=validate_sales_silver,
    op_kwargs={"target_dt": "{{ ds }}"}
    )

    check_bronze >> cleanup_task >> create_silver_sales >> insert_silver_sales >> validate_task

