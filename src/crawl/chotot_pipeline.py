from typing import Dict, List

import pandas as pd
from src.crawl.chotot_crawler import get_listing_ids, get_property_payload
from src.crawl.chotot_transformer import build_detail_record, build_fallback_record
from src.database.mongodb_repository import upsert_raw_listings_to_mongodb

# Import cấu hình
from config.settings import MONGO_COLLECTION_CHOTOT

def crawl_chotot_to_mongodb(pages: int = 1) -> int:
    """Pipeline lấy dữ liệu Chợ Tốt và lưu vào MongoDB."""
    total_saved = 0
    for page in range(1, pages + 1):
        print(f"[Listing] Đang cào trang {page}/{pages}...", flush=True)
        df_list = get_listing_ids(page=page)
        if df_list is None or df_list.empty:
            continue

        records: List[Dict] = []
        for _, row in df_list.iterrows():
            list_id = row.get("list_id")
            detail_payload = get_property_payload(str(list_id)) if list_id else None
            
            if detail_payload:
                # Dùng dữ liệu chi tiết
                record = build_detail_record(detail_payload)
            else:
                # Fallback dùng dữ liệu từ trang listing
                record = build_fallback_record(row)
                
            if record:
                records.append(record)

        if records:
            final_df = pd.DataFrame(records)
            # Sử dụng MONGO_COLLECTION_CHOTOT từ config
            page_saved = upsert_raw_listings_to_mongodb(final_df, collection_name=MONGO_COLLECTION_CHOTOT)
            total_saved += page_saved
            print(f"[Mongo] Trang {page}: ghi {page_saved} bản ghi | Lũy kế đã ghi: {total_saved}", flush=True)

    print(f"Hoàn tất. Đã ghi {total_saved} bản ghi vào MongoDB.", flush=True)
    return total_saved
