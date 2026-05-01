import sys
from pathlib import Path
import random
import time
from typing import Dict, Optional

# Cho phép chạy trực tiếp file
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import requests

# Import cấu hình
from config.selectors import CHOTOT_CONFIG

BASE_AD_LISTING_URL = CHOTOT_CONFIG["BASE_URL"]
CHOTOT_HEADERS = CHOTOT_CONFIG["HEADERS"]
REQUEST_TIMEOUT = 15
DETAIL_MAX_RETRIES = 2


def get_listing_ids(page: int = 1, region_v2: str = "12000", limit: int = 30) -> Optional[pd.DataFrame]:
    """Lấy danh sách tin từ Chợ Tốt API."""
    params = {
        "region_v2": region_v2,
        "cg": "1000",
        "limit": limit,
        "o": (page - 1) * limit,
        "st": "s,k",
    }

    try:
        resp = requests.get(BASE_AD_LISTING_URL, params=params, headers=CHOTOT_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        ads = resp.json().get("ads", [])
        if not ads:
            return pd.DataFrame()

        df = pd.DataFrame(ads)
        if "ad_id" not in df.columns:
            return pd.DataFrame()

        keep_cols = [
            c for c in [
                "ad_id", "list_id", "subject", "price", "area", "region_name",
                "area_name", "ward_name", "sub_area_name", "street_name", "address",
                "images", "image", "thumbnail_image", "webp_image", "url", "share_url",
            ] if c in df.columns
        ]
        return df[keep_cols].copy()
    except requests.RequestException as e:
        print(f"Lỗi mạng khi lấy danh sách trang {page}: {e}")
        return None


def get_property_payload(list_id: str) -> Optional[Dict]:
    """Lấy payload chi tiết thô theo list_id."""
    time.sleep(random.uniform(0.4, 1.0))
    url = f"{BASE_AD_LISTING_URL}/{list_id}"

    for attempt in range(1, DETAIL_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=CHOTOT_HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            if payload:
                return payload
            return None
        except requests.RequestException as e:
            if attempt == DETAIL_MAX_RETRIES:
                print(f"Lỗi bóc tách tin list_id={list_id}: {e}")
            else:
                time.sleep(0.5)
    return None

from config.settings import DEFAULT_PAGES_LOCAL

def crawl_chotot_to_mongodb(pages: int = DEFAULT_PAGES_LOCAL) -> int:
    from src.crawl.chotot_pipeline import crawl_chotot_to_mongodb as run_pipeline
    return run_pipeline(pages=pages)

if __name__ == "__main__":
    # Chạy cào thật và lưu vào MongoDB (Sử dụng giá trị mặc định từ config)
    crawl_chotot_to_mongodb()