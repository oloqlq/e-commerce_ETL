from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta

DATABASE_BRONZE = 'ecommerce_bronze_db'
DATABASE_SILVER = 'ecommerce_silver_db'
BUCKET          = 'de-ai-14-827913617635-ap-northeast-1-an'
SILVER_S3_PATH  = 's3://de-ai-14-827913617635-ap-northeast-1-an/silver/sales/'
ATHENA_RESULTS  = 's3://de-ai-14-827913617635-ap-northeast-1-an/athena-results/'
SILVER_TBL_NAME = 'silver_sales'

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


with DAG(
    dag_id="bronze_to_silver_sales",
    description="sales silver 테이블 구성 및 데이터 증분 작업",
    default_args={
        "owner":       "airflow",
        "retries":     1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "sales"],
) as dag:

    # t1: cleanup
    cleanup_task = PythonOperator(
        task_id="cleanup_silver_sales_partition",
        python_callable=cleanup_silver_sales_partition,
        op_kwargs={"target_dt": "{{ macros.ds_add(ds, -1) }}"},
    )

    # t2: 테이블 없으면 생성
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

    # t3: 어제 데이터만 INSERT
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
            ) = DATE('{{ ds }}') - INTERVAL '1' DAY;
        """,
        params={
            'database_bronze': DATABASE_BRONZE,
            'database_silver': DATABASE_SILVER,
            'tbl_name':        SILVER_TBL_NAME,
        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS,
    )

    cleanup_task >> create_silver_sales >> insert_silver_sales