from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

DATABASE_SILVER = 'ecommerce_silver_db'
DATABASE_GOLD   = 'ecommerce_gold_db'
GOLD_S3_PATH    = 's3://de-ai-14-827913617635-ap-northeast-1-an/gold/'
ATHENA_RESULTS  = 's3://de-ai-14-827913617635-ap-northeast-1-an/athena-results/'
BUCKET = 'de-ai-14-827913617635-ap-northeast-1-an'


def cleanup_gold_partition(target_dt, **kwargs):
    hook = S3Hook(aws_conn_id="aws_default")
    s3 = hook.get_conn()

    prefixes = [
        f"gold/funnel/event_date={target_dt}/",
        f"gold/device/event_date={target_dt}/",
        f"gold/search/event_date={target_dt}/",
        f"gold/campaign/event_date={target_dt}/",
        f"gold/item/event_date={target_dt}/",

        f"gold/sales_daily/dt={target_dt}/",
        f"gold/sales_product/dt={target_dt}/",
        f"gold/sales_category/dt={target_dt}/",
        f"gold/sales_user/dt={target_dt}/",
        f"gold/sales_payment/dt={target_dt}/",
        f"gold/sales_region/dt={target_dt}/",
    ]

    for prefix in prefixes:
        paginator = s3.get_paginator("list_objects_v2")
        batch = []

        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})

        if batch:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch})
            print(f"삭제 완료: {prefix} / {len(batch)}개 파일")


with DAG(
    dag_id="silver_to_gold",
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "event", "sales"],
) as dag:

    wait_for_event_silver = ExternalTaskSensor(
        task_id="wait_for_event_silver",
        external_dag_id="bronze_to_silver_event",
        external_task_id=None,
        allowed_states=["success"],
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
        execution_date_fn=lambda dt: dt,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    cleanup_gold = PythonOperator(
        task_id="cleanup_gold",
        python_callable=cleanup_gold_partition,
        op_kwargs={"target_dt": "{{ macros.ds_add(ds, -1) }}"}
    )

    # =========================
    # Event Gold Tables
    # =========================

    create_gold_funnel = AthenaOperator(
        task_id="create_gold_funnel",
        query="""
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
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_device = AthenaOperator(
        task_id="create_gold_device",
        query="""
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
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_search = AthenaOperator(
        task_id="create_gold_search",
        query="""
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
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_campaign = AthenaOperator(
        task_id="create_gold_campaign",
        query="""
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
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_item = AthenaOperator(
        task_id="create_gold_item",
        query="""
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
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    # =========================
    # Sales Gold Tables
    # =========================

    create_gold_sales_daily = AthenaOperator(
        task_id="create_gold_sales_daily",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_sales_daily (
                total_revenue      BIGINT,
                order_count        BIGINT,
                user_count         BIGINT,
                total_quantity     BIGINT,
                avg_order_amount   DOUBLE
            )
            PARTITIONED BY (order_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}sales_daily/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_sales_product = AthenaOperator(
        task_id="create_gold_sales_product",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_sales_product (
                item_id            STRING,
                total_revenue      BIGINT,
                order_count        BIGINT,
                total_quantity     BIGINT,
                avg_order_amount   DOUBLE
            )
            PARTITIONED BY (order_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}sales_product/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_sales_category = AthenaOperator(
        task_id="create_gold_sales_category",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_sales_category (
                category           STRING,
                total_revenue      BIGINT,
                order_count        BIGINT,
                total_quantity     BIGINT,
                avg_order_amount   DOUBLE
            )
            PARTITIONED BY (order_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}sales_category/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_sales_user = AthenaOperator(
        task_id="create_gold_sales_user",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_sales_user (
                user_id            STRING,
                order_count        BIGINT,
                total_spent        BIGINT,
                total_quantity     BIGINT,
                avg_order_amount   DOUBLE
            )
            PARTITIONED BY (order_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}sales_user/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_sales_payment = AthenaOperator(
        task_id="create_gold_sales_payment",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_sales_payment (
                payment_method     STRING,
                total_revenue      BIGINT,
                order_count        BIGINT,
                avg_order_amount   DOUBLE
            )
            PARTITIONED BY (order_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}sales_payment/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    create_gold_sales_region = AthenaOperator(
        task_id="create_gold_sales_region",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_gold }}.gold_sales_region (
                region             STRING,
                total_revenue      BIGINT,
                order_count        BIGINT,
                user_count         BIGINT,
                avg_order_amount   DOUBLE
            )
            PARTITIONED BY (order_date DATE)
            STORED AS PARQUET
            LOCATION '{{ params.gold_s3_path }}sales_region/'
            TBLPROPERTIES ('parquet.compress'='SNAPPY')
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "gold_s3_path": GOLD_S3_PATH,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    # =========================
    # Event Insert
    # =========================

    insert_gold_funnel = AthenaOperator(
        task_id="insert_gold_funnel",
        query="""
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
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    insert_gold_device = AthenaOperator(
        task_id="insert_gold_device",
        query="""
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
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    insert_gold_search = AthenaOperator(
        task_id="insert_gold_search",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_search
            SELECT
                search_keyword,
                COUNT(*) AS count_search_keyword,
                event_date
            FROM {{ params.database_silver }}.silver_event
            WHERE event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND action = 'search'
            GROUP BY search_keyword, event_date;
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    insert_gold_campaign = AthenaOperator(
        task_id="insert_gold_campaign",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_campaign
            SELECT
                campaign_id,
                COUNT(DISTINCT user_id) AS visit_user_count,
                COUNT(DISTINCT CASE WHEN action = 'purchase' THEN session_id END) AS purchase_count,
                ROUND(
                    COUNT(DISTINCT CASE WHEN action = 'purchase' THEN session_id END) * 100.0
                    / NULLIF(COUNT(DISTINCT session_id), 0), 2
                ) AS conversion_rate,
                event_date
            FROM {{ params.database_silver }}.silver_event
            WHERE event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND campaign_id IS NOT NULL
            GROUP BY campaign_id, event_date;
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    insert_gold_item = AthenaOperator(
        task_id="insert_gold_item",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_item
            SELECT
                e.item_id,
                COUNT(DISTINCT CASE WHEN e.action = 'view' THEN e.session_id END) AS view_count,
                COUNT(DISTINCT CASE WHEN e.action = 'add_to_cart' THEN e.session_id END) AS cart_count,
                COUNT(DISTINCT CASE WHEN e.action = 'purchase' THEN e.session_id END) AS purchase_count,
                ROUND(
                    COUNT(DISTINCT CASE WHEN e.action = 'purchase' THEN e.session_id END) * 100.0
                    / NULLIF(COUNT(DISTINCT CASE WHEN e.action = 'view' THEN e.session_id END), 0), 2
                ) AS conversion_rate,
                COALESCE(CAST(SUM(s.total_amount) AS BIGINT), 0) AS total_revenue,
                ROUND(COALESCE(AVG(s.total_amount), 0), 2) AS avg_order_amount,
                e.event_date
            FROM {{ params.database_silver }}.silver_event e
            LEFT JOIN {{ params.database_silver }}.silver_sales s
                ON e.item_id = s.item_id
               AND e.user_id = s.user_id
               AND e.action = 'purchase'
            WHERE e.event_date = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND e.item_id IS NOT NULL
            GROUP BY e.item_id, e.event_date;
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    # =========================
    # Sales Insert
    # =========================
    # 일별 매출 KPI 대시보드
    # 총 매출, 주문 수, 구매 고객 수, 총 판매 수량, 평균 주문 금액을 보여주기 위한 집계
    insert_gold_sales_daily = AthenaOperator(
        task_id="insert_gold_sales_daily",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_sales_daily
            SELECT
                CAST(SUM(total_amount) AS BIGINT) AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                COUNT(DISTINCT user_id) AS user_count,
                CAST(SUM(quantity) AS BIGINT) AS total_quantity,
                ROUND(AVG(total_amount), 2) AS avg_order_amount,
                DATE(order_time) AS order_date
            FROM {{ params.database_silver }}.silver_sales
            WHERE DATE(order_time) = DATE('{{ ds }}') - INTERVAL '1' DAY
            GROUP BY DATE(order_time);
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )
    # 상품별 매출 성과 대시보드
    # 상품별 매출, 주문 수, 판매 수량, 평균 주문 금액을 비교하기 위한 집계
    insert_gold_sales_product = AthenaOperator(
        task_id="insert_gold_sales_product",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_sales_product
            SELECT
                item_id,
                CAST(SUM(total_amount) AS BIGINT) AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                CAST(SUM(quantity) AS BIGINT) AS total_quantity,
                ROUND(AVG(total_amount), 2) AS avg_order_amount,
                DATE(order_time) AS order_date
            FROM {{ params.database_silver }}.silver_sales
            WHERE DATE(order_time) = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND item_id IS NOT NULL
            GROUP BY item_id, DATE(order_time);
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    # 카테고리별 매출 분석 대시보드
    # 카테고리별 매출 비중, 주문 수, 판매 수량을 보여주기 위한 집계
    insert_gold_sales_category = AthenaOperator(
        task_id="insert_gold_sales_category",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_sales_category
            SELECT
                category,
                CAST(SUM(total_amount) AS BIGINT) AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                CAST(SUM(quantity) AS BIGINT) AS total_quantity,
                ROUND(AVG(total_amount), 2) AS avg_order_amount,
                DATE(order_time) AS order_date
            FROM {{ params.database_silver }}.silver_sales
            WHERE DATE(order_time) = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND category IS NOT NULL
            GROUP BY category, DATE(order_time);
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )
    # 고객별 구매 분석 대시보드
    # 고객별 주문 횟수, 총 구매 금액, 구매 수량, 평균 주문 금액을 분석하기 위한 집계
    insert_gold_sales_user = AthenaOperator(
        task_id="insert_gold_sales_user",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_sales_user
            SELECT
                user_id,
                COUNT(DISTINCT order_id) AS order_count,
                CAST(SUM(total_amount) AS BIGINT) AS total_spent,
                CAST(SUM(quantity) AS BIGINT) AS total_quantity,
                ROUND(AVG(total_amount), 2) AS avg_order_amount,
                DATE(order_time) AS order_date
            FROM {{ params.database_silver }}.silver_sales
            WHERE DATE(order_time) = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND user_id IS NOT NULL
            GROUP BY user_id, DATE(order_time);
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )


    # 결제수단별 매출 분석 대시보드
    # 카드, 간편결제 등 결제수단별 매출과 주문 수를 비교하기 위한 집계
    insert_gold_sales_payment = AthenaOperator(
        task_id="insert_gold_sales_payment",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_sales_payment
            SELECT
                payment_method,
                CAST(SUM(total_amount) AS BIGINT) AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                ROUND(AVG(total_amount), 2) AS avg_order_amount,
                DATE(order_time) AS order_date
            FROM {{ params.database_silver }}.silver_sales
            WHERE DATE(order_time) = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND payment_method IS NOT NULL
            GROUP BY payment_method, DATE(order_time);
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    # 지역별 매출 분석 대시보드
    # 지역별 매출, 주문 수, 구매 고객 수, 평균 주문 금액을 비교하기 위한 집계
    insert_gold_sales_region = AthenaOperator(
        task_id="insert_gold_sales_region",
        query="""
            INSERT INTO {{ params.database_gold }}.gold_sales_region
            SELECT
                region,
                CAST(SUM(total_amount) AS BIGINT) AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                COUNT(DISTINCT user_id) AS user_count,
                ROUND(AVG(total_amount), 2) AS avg_order_amount,
                DATE(order_time) AS order_date
            FROM {{ params.database_silver }}.silver_sales
            WHERE DATE(order_time) = DATE('{{ ds }}') - INTERVAL '1' DAY
              AND region IS NOT NULL
            GROUP BY region, DATE(order_time);
        """,
        params={
            "database_gold": DATABASE_GOLD,
            "database_silver": DATABASE_SILVER,
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
    )

    # =========================
    # Dependencies
    # =========================

    [wait_for_event_silver, wait_for_sales_silver] >> cleanup_gold >> [
        create_gold_funnel,
        create_gold_device,
        create_gold_search,
        create_gold_campaign,
        create_gold_item,
        create_gold_sales_daily,
        create_gold_sales_product,
        create_gold_sales_category,
        create_gold_sales_user,
        create_gold_sales_payment,
        create_gold_sales_region,
    ]

    create_gold_funnel >> insert_gold_funnel
    create_gold_device >> insert_gold_device
    create_gold_search >> insert_gold_search
    create_gold_campaign >> insert_gold_campaign
    create_gold_item >> insert_gold_item

    create_gold_sales_daily >> insert_gold_sales_daily
    create_gold_sales_product >> insert_gold_sales_product
    create_gold_sales_category >> insert_gold_sales_category
    create_gold_sales_user >> insert_gold_sales_user
    create_gold_sales_payment >> insert_gold_sales_payment
    create_gold_sales_region >> insert_gold_sales_region