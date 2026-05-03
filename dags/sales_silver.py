from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from datetime import datetime, timedelta
from dotenv import load_dotenv

import boto3
import pandas as pd
import io
import os
import time

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
ATHENA_OUTPUT_LOCATION = os.getenv("ATHENA_OUTPUT_LOCATION",)
DATABASE_BRONZE = os.getenv("DATABASE_BRONZE")
BRONZE_SALES_TABLE = os.getenv("BRONZE_SALES_TABLE")
PRODUCT_MASTER_TABLE = os.getenv("PRODUCT_MASTER_TABLE")
S3_BUCKET = "de-ai-14-827913617635-ap-northeast-1-an"
TEMP_PREFIX = "silver/sales_temp/"
FINAL_PREFIX = "silver/sales/"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def _get_session():
    return boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1"),
    )

def _split_s3_uri(s3_uri: str):
    no_schema = s3_uri.replace("s3://", "", 1)
    bucket, key = no_schema.split("/", 1)
    return bucket, key

def _delete_prefix(s3_client, bucket: str, prefix: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    batch = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                batch = []
    if batch:
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

def _run_athena_query_to_df(sql: str, database: str) -> pd.DataFrame:
    session = _get_session()
    s3_client = session.client("s3")
    athena_client = session.client("athena")
    resp = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]

    while True:
        result = athena_client.get_query_execution(QueryExecutionId=qid)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(3)

    output_s3 = result["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = _split_s3_uri(output_s3)
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.DataFrame() if len(body) == 0 else pd.read_csv(io.BytesIO(body))

def _upload_parquet(df: pd.DataFrame, s3_uri: str):
    bucket, key = _split_s3_uri(s3_uri)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    _get_session().client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def _read_parquet(s3_uri: str) -> pd.DataFrame:
    bucket, key = _split_s3_uri(s3_uri)
    body = _get_session().client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body), engine="pyarrow")

def cleanup_previous_partitions(target_dt: str, **kwargs):
    s3_client = _get_session().client("s3")

    for prefix in [f"{TEMP_PREFIX}dt={target_dt}/", f"{FINAL_PREFIX}dt={target_dt}/"]:
        _delete_prefix(s3_client, S3_BUCKET, prefix)
        print(f"[cleanup] s3://{S3_BUCKET}/{prefix}")

def extract_purchase_to_temp(target_dt: str, temp_file_uri: str, **kwargs):
    sql = f"""
        SELECT
            event_id,
            event_timestamp,
            user_id,
            region,
            item_id,
            discount_amount,
            payment_method
        FROM {DATABASE_BRONZE}.{BRONZE_SALES_TABLE}
        WHERE action = 'purchase'
          AND DATE(CAST(event_timestamp AS TIMESTAMP)) = DATE '{target_dt}'
    """
    df = _run_athena_query_to_df(sql=sql, database=DATABASE_BRONZE)
    if df.empty:
        raise AirflowSkipException(f"No purchase data for {target_dt}")
    _upload_parquet(df, temp_file_uri)
    print(f"[extract] saved {len(df)} rows -> {temp_file_uri}")

def transform_and_load_sales_silver(target_dt: str, temp_file_uri: str, final_file_uri: str, **kwargs):
    df = _read_parquet(temp_file_uri)
    if df.empty:
        raise AirflowSkipException(f"Temp parquet is empty for {target_dt}")
    product_sql = f"""
        SELECT item_id, category, CAST(price AS INTEGER) AS unit_price
        FROM {DATABASE_BRONZE}.{PRODUCT_MASTER_TABLE}
    """
    df_product = _run_athena_query_to_df(sql=product_sql, database=DATABASE_BRONZE)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0)

    df = df.merge(df_product, on="item_id", how="left")
    df_final = df.assign(
        order_id=df["event_id"],
        order_time=df["event_timestamp"],
        total_amount=df["unit_price"] - df["discount_amount"],
    )[[
        "order_id", "user_id", "order_time", "region",
        "item_id", "category", "unit_price", "discount_amount",
        "total_amount", "payment_method",
    ]].copy()
    _upload_parquet(df_final, final_file_uri)
    print(f"[transform] saved {len(df_final)} rows -> {final_file_uri}")

_TARGET_DT = "{{ data_interval_start | ds }}"

_TEMP_BASE_URI = f"s3://{S3_BUCKET}/silver/sales_temp"
_FINAL_BASE_URI = f"s3://{S3_BUCKET}/silver/sales"

_TEMP_URI = f"{_TEMP_BASE_URI}/dt={_TARGET_DT}/sales_temp_{_TARGET_DT}.parquet"
_FINAL_URI = f"{_FINAL_BASE_URI}/dt={_TARGET_DT}/sales_silver_{_TARGET_DT}.parquet"

with DAG(
    dag_id="bronze_to_silver_sales",
    description="bronze_sales -> silver_sales daily batch",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "sales"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_previous_partitions",
        python_callable=cleanup_previous_partitions,
        op_kwargs={"target_dt": _TARGET_DT},
    )

    extract_task = PythonOperator(
        task_id="extract_purchase_to_temp",
        python_callable=extract_purchase_to_temp,
        op_kwargs={"target_dt": _TARGET_DT, "temp_file_uri": _TEMP_URI},
    )

    transform_load_task = PythonOperator(
        task_id="transform_and_load_sales_silver",
        python_callable=transform_and_load_sales_silver,
        op_kwargs={"target_dt": _TARGET_DT, "temp_file_uri": _TEMP_URI, "final_file_uri": _FINAL_URI},
    )

    cleanup_task >> extract_task >> transform_load_task