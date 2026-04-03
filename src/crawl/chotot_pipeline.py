"""Tầng điều phối cho pipeline crawl raw của Chợ Tốt."""

import random
import time
from typing import Dict, List, Optional

import pandas as pd

from src.crawl.chotot_crawler import get_listing_ids, get_property_payload
from src.crawl.chotot_transformer import build_detail_record, build_fallback_record
from src.database.mongodb_repository import upsert_raw_listings_to_mongodb


def process_listing_page(df: pd.DataFrame) -> pd.DataFrame:
    """Lấy payload chi tiết và biến một trang listing thành bản ghi raw."""
    print("Đang bóc tách chi tiết tin đăng...")
    if df.empty or "list_id" not in df.columns:
        return pd.DataFrame()

    records: List[Dict] = []
    skipped_missing_list_id = 0

    for _, row in df.iterrows():
        list_id = row.get("list_id")
        ad_id = row.get("ad_id")
        if list_id is None or str(list_id).strip() == "":
            skipped_missing_list_id += 1
            continue

        payload = get_property_payload(str(list_id))
        detail = build_detail_record(payload=payload, ad_id=str(ad_id) if ad_id is not None else None, list_id=str(list_id)) if payload else None

        if detail is None:
            fallback = build_fallback_record(row)
            if fallback is not None:
                records.append(fallback)
        else:
            records.append(detail)

    if skipped_missing_list_id:
        print(f"[Detail] Bỏ qua {skipped_missing_list_id} tin do thiếu list_id.")

    out_df = pd.DataFrame(records)
    if out_df.empty:
        return pd.DataFrame()
    return out_df


def crawl_chotot_to_mongodb(pages: int = 2) -> int:
    """Cào theo từng trang -> biến đổi -> upsert vào MongoDB."""
    print(f"Bắt đầu crawl Chợ Tốt ({pages} trang)...")

    total_saved = 0
    total_seen = 0
    seen_ad_ids = set()

    for page in range(1, pages + 1):
        print(f"[Listing] Đang cào trang {page}/{pages}...")
        page_df = get_listing_ids(page=page)

        if page_df is None:
            print(f"[Listing] Trang {page}/{pages} lỗi mạng, bỏ qua và tiếp tục trang sau.")
            time.sleep(random.uniform(1.2, 2.0))
            continue

        if page_df.empty:
            print(f"[Listing] Trang {page}/{pages} không có dữ liệu, dừng sớm.")
            break

        raw_count = len(page_df)
        print(f"[Listing] Trang {page}/{pages}: lấy được {raw_count} tin.")
        total_seen += raw_count

        dedup_same_page_df = page_df.drop_duplicates(subset=["ad_id"])
        dropped_same_page = raw_count - len(dedup_same_page_df)

        page_df = dedup_same_page_df[~dedup_same_page_df["ad_id"].astype(str).isin(seen_ad_ids)]
        dropped_seen_before = len(dedup_same_page_df) - len(page_df)

        if dropped_same_page or dropped_seen_before:
            print(
                f"[Listing] Trang {page}/{pages}: lọc trùng "
                f"trong trang={dropped_same_page}, trùng trang trước={dropped_seen_before}."
            )

        if page_df.empty:
            print(f"[Listing] Trang {page}/{pages}: toàn bộ tin bị trùng, bỏ qua.")
            time.sleep(random.uniform(1.2, 2.0))
            continue

        seen_ad_ids.update(page_df["ad_id"].astype(str).tolist())
        print(f"[Detail] Trang {page}/{pages}: bóc tách {len(page_df)} tin...")

        detail_df = process_listing_page(page_df)
        if detail_df.empty:
            print(f"[Detail] Trang {page}/{pages}: không có bản ghi hợp lệ sau bóc tách/fallback từng tin.")
            time.sleep(random.uniform(1.2, 2.0))
            continue

        page_saved = upsert_raw_listings_to_mongodb(detail_df)
        total_saved += page_saved
        print(
            f"[Mongo] Trang {page}/{pages}: ghi {page_saved} bản ghi | "
            f"Lũy kế đã ghi: {total_saved}"
        )

        time.sleep(random.uniform(1.2, 2.0))

    if total_seen == 0:
        print("Không có dữ liệu danh sách để crawl.")
        return 0

    print(f"Hoàn tất. Đã ghi {total_saved} bản ghi vào MongoDB.")
    return total_saved
