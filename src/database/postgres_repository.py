"""ETL orchestrator — chuyển raw listings từ MongoDB sang PostgreSQL.
"""

import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Cho phép chạy trực tiếp file: Luôn đặt lên đầu
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import cấu hình
from config.settings import MONGO_COLLECTION_BDS, MONGO_COLLECTION_CHOTOT

from src.database.address_cleaner import clean_address_text, normalize_text
from src.database.dim_repository import (
    load_dim_maps,
    prepare_district_seed_rows,
    resolve_source_id,
    upsert_dim_districts,
)
from src.database.fact_repository import (
    backfill_fact_address_text,
    build_fact_row,
    extract_raw_fields,
    insert_quarantine_rows,
    prune_stale_source_rows,
    upsert_fact_rows,
)
from src.database.mongodb_connect import MongoDBConnect
from src.database.postgres_connect import PostgreSQLConnect
from src.database.run_log_repository import ensure_run, finish_run
from src.database.schema_manager import ensure_postgres_schema


@dataclass
class LoadStats:
    records_read: int = 0
    records_loaded: int = 0
    records_quarantined: int = 0


def _load_raw_docs(source: Optional[str] = None, collection_name: str = "raw_listings") -> List[Dict[str, Any]]:
    with MongoDBConnect.from_env() as mongo:
        db = mongo.db
        query: Dict[str, Any] = {}
        if source:
            query["source"] = source
        cursor = db[collection_name].find(query)
        return list(cursor)


def debug_address_pipeline(limit: int = 5, source: Optional[str] = None) -> None:
    """Trace mất dấu qua từng tầng để biết chính xác vấn đề xảy ra ở đâu."""
    docs = _load_raw_docs(source=source)[:limit]
    print(f"\n{'='*70}")
    print(f"DEBUG ADDRESS PIPELINE — {len(docs)} records")
    print(f"{'='*70}\n")
    for i, doc in enumerate(docs, 1):
        raw = extract_raw_fields(doc)
        print(f"[{i}] external_id={raw['external_id']}  source={raw['source']}")
        print(f"    MONGO address  : {raw['address']!r}")
        print(f"    MONGO ward     : {raw['ward']!r}")
        print(f"    MONGO district : {raw['district']!r}")
        print(f"    MONGO city     : {raw['city']!r}")
        result = clean_address_text(
            raw["address"], raw["ward"], raw["district"], raw["city"], 
            raw["source_url"], raw["title"], raw["description"]
        )
        print(f"    → address_text : {result!r}")
        print()


def load_raw_listings_to_postgres(
    source: Optional[str] = None,
    collection_name: str = "raw_listings",
    dag_id: str = "mongo_to_postgres",
    run_type: str = "manual",
) -> Dict[str, int]:
    """Chuyển raw listings từ MongoDB sang PostgreSQL fact/dim tables."""
    docs = _load_raw_docs(source=source, collection_name=collection_name)
    stats = LoadStats(records_read=len(docs))

    if not docs:
        return {"records_read": 0, "records_loaded": 0, "records_quarantined": 0}

    with PostgreSQLConnect.from_env() as pg:
        ensure_postgres_schema(pg)
        run_id = ensure_run(pg, dag_id=dag_id, run_type=run_type)
        dim_maps = load_dim_maps(pg)

        source_id_for_prune: Optional[int] = None
        if source:
            source_id_for_prune = resolve_source_id(source, dim_maps["source"])

        district_seed_rows = prepare_district_seed_rows(docs)
        upsert_dim_districts(pg, district_seed_rows)
        dim_maps = load_dim_maps(pg)

        fact_rows: List[Dict[str, Any]] = []
        quarantine_rows: List[Dict[str, Any]] = []

        for doc in docs:
            fact_row, quarantine_row = build_fact_row(pg, doc, dim_maps)
            if fact_row is not None:
                fact_row["run_id"] = run_id
                fact_rows.append(fact_row)
            elif quarantine_row is not None:
                quarantine_rows.append(quarantine_row)

        if fact_rows:
            stats.records_loaded = upsert_fact_rows(pg, fact_rows)

        if source_id_for_prune is not None:
            kept_urls = [str(row.get("listing_url")) for row in fact_rows if row.get("listing_url")]
            prune_stale_source_rows(pg, source_id_for_prune, kept_urls, stats.records_read)

        if quarantine_rows:
            stats.records_quarantined = insert_quarantine_rows(pg, run_id, quarantine_rows)

        backfill_fact_address_text(pg)

        status = "partial" if stats.records_quarantined else "success"
        finish_run(
            pg, run_id=run_id, status=status,
            records_read=stats.records_read,
            records_loaded=stats.records_loaded,
            records_quarantined=stats.records_quarantined,
        )

    return {
        "records_read": stats.records_read,
        "records_loaded": stats.records_loaded,
        "records_quarantined": stats.records_quarantined,
    }


def load_all_sources_to_postgres(collection_name: str = "raw_listings") -> None:
    """Load toàn bộ raw listings theo từng source và in ra kết quả dạng dict."""
    combined = {"records_read": 0, "records_loaded": 0, "records_quarantined": 0}
    for source in ("chotot", "batdongsan"):
        print(f"--- Loading {source} to Postgres ---")
        result = load_raw_listings_to_postgres(source=source, collection_name=collection_name)
        print(f"Done {source}: {result}")
        combined["records_read"] += result["records_read"]
        combined["records_loaded"] += result["records_loaded"]
        combined["records_quarantined"] += result["records_quarantined"]
    print(f"\nFinal Result: {combined}")


if __name__ == "__main__":
    load_all_sources_to_postgres()