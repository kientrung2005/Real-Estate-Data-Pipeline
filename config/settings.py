import os

# --- CẤU HÌNH CRAWLER ---
DEFAULT_PAGES_LOCAL = 3
DEFAULT_PAGES_AIRFLOW = 1

SLEEP_FAST_MODE = (1.0, 2.5)
SLEEP_SAFE_MODE = (3.0, 7.0)

BROWSER_PROFILE_NAME = "browser_profile"
DEFAULT_HEADLESS = False

# --- CẤU HÌNH CƠ SỞ DỮ LIỆU ---
MONGO_DB_NAME = "real_estate_raw"
MONGO_COLLECTION_BDS = "batdongsan"
MONGO_COLLECTION_CHOTOT = "chotot"

POSTGRES_TABLE_LISTINGS = "listings"
POSTGRES_TABLE_DISTRICTS = "dim_district"

# --- CẤU HÌNH LOGGING ---
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# --- ĐƯỜNG DẪN HỆ THỐNG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR_LOCAL = os.path.join(BASE_DIR, "logs")
LOG_DIR_AIRFLOW = "/opt/airflow/logs"
