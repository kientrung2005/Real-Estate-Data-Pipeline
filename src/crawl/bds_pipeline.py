import time
import random
from typing import List, Dict

import pandas as pd
from src.crawl.bds_crawler import BDSCrawler
from src.crawl.bds_transformer import build_bds_record
from src.database.mongodb_repository import upsert_raw_listings_to_mongodb

# Import cấu hình
from config.settings import MONGO_COLLECTION_BDS, DEFAULT_PAGES_LOCAL

def crawl_bds_to_mongodb(pages: int = DEFAULT_PAGES_LOCAL, fetch_detail: bool = True, headless: bool = True, user_data_dir: str = None) -> int:
    """Cào danh sách từ batdongsan.com.vn và upsert vào MongoDB."""
    print(f"Bắt đầu crawl Batdongsan.com.vn ({pages} trang)...", flush=True)
    
    total_saved = 0
    seen_ad_ids = set()
    
    # Khởi tạo crawler với tham số headless truyền vào
    crawler = BDSCrawler(headless=headless, user_data_dir=user_data_dir)
    
    try:
        for page in range(1, pages + 1):
            print(f"[Listing] Đang cào trang {page}/{pages}...", flush=True)
            df_list = crawler.get_listing_urls(page_num=page)
            
            if df_list is None or df_list.empty:
                print(f"[Listing] Trang {page} không có dữ liệu, dừng sớm hoặc bỏ qua.", flush=True)
                time.sleep(random.uniform(2.0, 4.0))
                continue
                
            raw_count = len(df_list)
            print(f"[Listing] Lấy được {raw_count} tin từ trang {page}.", flush=True)
            
            page_df = df_list[~df_list["ad_id"].astype(str).isin(seen_ad_ids)]
            if page_df.empty:
                print(f"[Listing] Trang {page}: toàn bộ tin bị trùng.", flush=True)
                continue
                
            seen_ad_ids.update(page_df["ad_id"].astype(str).tolist())
            
            records: List[Dict] = []
            
            for _, row in page_df.iterrows():
                row_dict = row.to_dict()
                detail_dict = None
                
                if fetch_detail and row_dict.get("url"):
                    detail_dict = crawler.get_property_detail(row_dict["url"])
                    
                record = build_bds_record(row_data=row_dict, detail_data=detail_dict)
                if record:
                    records.append(record)
                    
            if not records:
                continue
                
            final_df = pd.DataFrame(records)
            # Sử dụng MONGO_COLLECTION_BDS từ config
            page_saved = upsert_raw_listings_to_mongodb(final_df, collection_name=MONGO_COLLECTION_BDS)
            total_saved += page_saved
            print(f"[Mongo] Trang {page}/{pages}: Ghi {page_saved} bản ghi vào MongoDB. Lũy kế: {total_saved}", flush=True)
            
            time.sleep(random.uniform(2.0, 4.0))
            
    except Exception as e:
        print(f"Lỗi khi chạy pipeline bds: {e}", flush=True)
    finally:
        crawler.close()
        
    print(f"Hoàn tất crawl batdongsan.com.vn. Đã ghi {total_saved} tin.", flush=True)
    return total_saved
