"""Các hàm repository MongoDB cho việc lưu raw listing."""

import logging
from datetime import UTC, datetime

import pandas as pd
from pymongo import UpdateOne

from src.database.mongodb_connect import MongoDBConnect

logger = logging.getLogger(__name__)


def upsert_raw_listings_to_mongodb(df: pd.DataFrame, collection_name: str = "raw_listings") -> int:
    """Upsert bản ghi raw theo khóa source + external_id."""
    if df.empty:
        return 0

    docs = df.to_dict(orient="records")
    operations = []
    for doc in docs:
        external_id = doc.get("external_id")
        source = doc.get("source")
        if not external_id or not source:
            continue
        first_crawled_at = doc.get("first_crawled_at") or doc.get("crawled_at") or datetime.now(UTC).isoformat()
        operations.append(
            UpdateOne(
                {"source": source, "external_id": external_id},
                {
                    "$set": doc,
                    "$setOnInsert": {"first_crawled_at": first_crawled_at},
                },
                upsert=True,
            )
        )

    if not operations:
        return 0

    with MongoDBConnect.from_env() as mongo:
        db = mongo.db
        result = db[collection_name].bulk_write(operations, ordered=False)

    total = result.upserted_count + result.modified_count
    if result.matched_count > 0 and total == 0:
        logger.info(
            "MongoDB: %d khớp nhưng 0 thay đổi (dữ liệu giống nhau)",
            result.matched_count,
        )
    return total


def get_locations_master_data(collection_name: str = "raw_listings", source: str = "chotot") -> pd.DataFrame:
    """Đọc danh sách city/district/ward thô từ MongoDB để Silver layer tự normalize."""
    with MongoDBConnect.from_env() as mongo:
        db = mongo.db
        cursor = db[collection_name].aggregate(
            [
                {"$match": {"source": source}},
                {
                    "$project": {
                        "city": "$city",
                        "district": "$district",
                        "ward": "$ward",
                    }
                },
                {
                    "$match": {
                        "$or": [
                            {"district": {"$ne": None}},
                            {"ward": {"$ne": None}},
                        ]
                    }
                },
            ]
        )
        rows = list(cursor)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in ["city", "district", "ward"]:
        if col not in df.columns:
            df[col] = None

    return df[["city", "district", "ward"]].drop_duplicates()
