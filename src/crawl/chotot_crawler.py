"""Tầng HTTP cho các endpoint của Chợ Tốt."""

import random
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

# Cho phép chạy trực tiếp file: python src/crawl/chotot_crawler.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CHOTOT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.nhatot.com",
    "Referer": "https://www.nhatot.com/",
}

BASE_AD_LISTING_URL = "https://gateway.chotot.com/v1/public/ad-listing"
REQUEST_TIMEOUT = 15
DETAIL_MAX_RETRIES = 2


def get_listing_ids(page: int = 1, region_v2: str = "12000", limit: int = 20) -> Optional[pd.DataFrame]:
    """Lấy danh sách tin và các trường nhẹ của một trang."""
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
            c
            for c in [
                "ad_id",
                "list_id",
                "subject",
                "price",
                "area",
                "region_name",
                "area_name",
                "ward_name",
                "sub_area_name",
                "street_name",
                "address",
                "images",
                "image",
                "thumbnail_image",
                "webp_image",
            ]
            if c in df.columns
        ]
        return df[keep_cols].copy()
    except requests.RequestException as e:
        print(f"Lỗi mạng khi lấy danh sách trang {page}: {e}")
        return None


def get_property_payload(list_id: str) -> Optional[Dict]:
    """Lấy payload chi tiết thô theo list_id và thử lại khi lỗi."""
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


def crawl_chotot_to_mongodb(pages: int = 2) -> int:
    """Wrapper tương thích ngược, chuyển điều phối sang tầng pipeline."""
    from src.crawl.chotot_pipeline import crawl_chotot_to_mongodb as run_pipeline

    return run_pipeline(pages=pages)


if __name__ == "__main__":
    crawl_chotot_to_mongodb(pages=3)