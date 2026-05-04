from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import các function pipeline của mình
from src.crawl.chotot_pipeline import crawl_chotot_to_mongodb
from src.crawl.bds_pipeline import crawl_bds_to_mongodb
from src.database.postgres_repository import load_raw_listings_to_postgres

# Cấu hình mặc định cho các task
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Import cấu hình
from config.settings import DEFAULT_PAGES_AIRFLOW

# Khởi tạo DAG chạy hằng ngày vào lúc 2 giờ sáng
with DAG(
    "real_estate_pipeline",
    default_args=default_args,
    description="Pipeline cào và transform dữ liệu BĐS hằng ngày",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["real_estate", "etl"],
) as dag:

    # Task 1: Crawl Chợ Tốt
    crawl_chotot_task = PythonOperator(
        task_id="crawl_chotot_to_mongodb",
        python_callable=crawl_chotot_to_mongodb,
        op_kwargs={"pages": DEFAULT_PAGES_AIRFLOW},
    )

    # Task 2: Crawl Batdongsan
    crawl_bds_task = PythonOperator(
        task_id="crawl_bds_to_mongodb",
        python_callable=crawl_bds_to_mongodb,
        op_kwargs={"pages": DEFAULT_PAGES_AIRFLOW, "fetch_detail": True, "headless": True},
    )

    # Task 3: Load Chợ Tốt từ MongoDB sang Postgres
    load_chotot_postgres_task = PythonOperator(
        task_id="load_chotot_to_postgres",
        python_callable=load_raw_listings_to_postgres,
        op_kwargs={"source": "chotot", "dag_id": "real_estate_pipeline", "run_type": "scheduled"},
    )

    # Task 4: Load Batdongsan từ MongoDB sang Postgres
    load_bds_postgres_task = PythonOperator(
        task_id="load_bds_to_postgres",
        python_callable=load_raw_listings_to_postgres,
        op_kwargs={"source": "batdongsan", "dag_id": "real_estate_pipeline", "run_type": "scheduled"},
    )

    # Thiết lập thứ tự chạy: Crawl xong mới Load
    crawl_chotot_task >> load_chotot_postgres_task
    crawl_bds_task >> load_bds_postgres_task