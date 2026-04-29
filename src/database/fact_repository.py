"""CRUD operations cho fact tables và quarantine trong PostgreSQL."""

from __future__ import annotations

import json
import logging
import math
import re
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from src.database.address_cleaner import clean_address_text, normalize_text
from src.database.dim_repository import (
    PROPERTY_TYPE_MAP,
    resolve_area_band,
    resolve_district_id,
    resolve_price_band,
    resolve_property_type,
    resolve_source_id,
)
from src.database.postgres_connect import PostgreSQLConnect

logger = logging.getLogger(__name__)



def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if value is None:
        return datetime.now(UTC)
    text = str(value).strip()
    if not text:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return datetime.now(UTC)


def _parse_json_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            return {"value": payload}
    return {}


def extract_raw_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    raw_payload = _parse_json_payload(doc.get("raw_payload"))
    return {
        "source": doc.get("source"),
        "external_id": doc.get("external_id"),
        "source_url": doc.get("source_url") or doc.get("listing_url"),
        "title": doc.get("title"),
        "description": doc.get("description"),
        "price_vnd": doc.get("price_vnd"),
        "area_sqm": doc.get("area_sqm"),
        "address": doc.get("address"),
        "district": doc.get("district"),
        "ward": doc.get("ward"),
        "city": doc.get("city"),
        "property_type": doc.get("property_type"),
        "transaction_type": doc.get("transaction_type"),
        "contact_name": doc.get("contact_name"),
        "contact_phone": doc.get("contact_phone"),
        "images": doc.get("images") or [],
        "raw_payload": raw_payload,
        "crawled_at": _parse_timestamp(doc.get("crawled_at")),
    }


def _price_million(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
        if math.isnan(number):
            return None
        return round(number / 1_000_000, 2)
    except (TypeError, ValueError):
        return None


def _price_per_sqm_million(price_million: Optional[float], area_sqm: Optional[float]) -> Optional[float]:
    if price_million is None or area_sqm in (None, 0):
        return None
    try:
        return round(price_million / float(area_sqm), 4)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


# Build fact row

def build_fact_row(
    doc: Dict[str, Any], dim_maps: Dict[str, Dict[str, Any]]
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Xây dựng fact row từ raw doc. Trả về (fact_row, None) hoặc (None, quarantine_row)."""
    raw = extract_raw_fields(doc)
    external_id = raw["external_id"]
    source = raw["source"]

    if not external_id or not source or not raw["source_url"]:
        return None, {
            "source_name": source,
            "listing_url": raw["source_url"],
            "error_stage": "ingest",
            "error_code": "MISSING_KEY",
            "error_message": "Thiếu source, external_id hoặc listing_url trong raw listing",
            "raw_payload": raw["raw_payload"],
        }

    source_id = resolve_source_id(source, dim_maps["source"])
    if source_id is None:
        return None, {
            "source_name": source,
            "listing_url": raw["source_url"],
            "error_stage": "load",
            "error_code": "UNKNOWN_SOURCE",
            "error_message": f"Không tìm thấy source_id cho source={source}",
            "raw_payload": raw["raw_payload"],
        }

    crawled_at = raw["crawled_at"]
    date_key = int(crawled_at.strftime("%Y%m%d"))
    price_million = _price_million(raw["price_vnd"])
    area_sqm = raw["area_sqm"]

    try:
        if area_sqm is None:
            area_sqm_val = None
        else:
            area_number = float(area_sqm)
            area_sqm_val = None if math.isnan(area_number) else round(area_number, 2)
    except (TypeError, ValueError):
        area_sqm_val = None

    if price_million is None:
        return None, {
            "source_name": source,
            "listing_url": raw["source_url"],
            "error_stage": "transform",
            "error_code": "MISSING_PRICE",
            "error_message": "Thiếu price_vnd nên không thể tính price_million_vnd/price_band_id",
            "raw_payload": raw["raw_payload"],
        }

    if area_sqm_val is None or area_sqm_val == 0:
        return None, {
            "source_name": source,
            "listing_url": raw["source_url"],
            "error_stage": "transform",
            "error_code": "MISSING_AREA",
            "error_message": "Thiếu area_sqm hợp lệ nên không thể tính area_band_id/price_per_sqm_million",
            "raw_payload": raw["raw_payload"],
        }

    # Giải quyết district
    district_id = resolve_district_id(raw["district"], dim_maps["district"])
    resolved_district_name = raw["district"]

    if district_id is None:
        search_text = normalize_text(
            " ".join(filter(None, [
                str(raw.get("address", "")),
                str(raw.get("ward", "")),
                str(raw.get("source_url", "")),
            ]))
        )
        if search_text:
            for district_key, d_id in dim_maps["district"].items():
                if district_key in search_text:
                    district_id = d_id
                    resolved_district_name = district_key
                    break

    fact_row = {
        "run_id": None,
        "source_id": source_id,
        "external_id": external_id,
        "district_id": district_id,
        "type_id": resolve_property_type(raw["property_type"], dim_maps["property_type"]),
        "date_key": date_key,
        "price_band_id": resolve_price_band(price_million, dim_maps["price_band"]),
        "area_band_id": resolve_area_band(area_sqm_val, dim_maps["area_band"]),
        "title": raw["title"],
        "listing_url": raw["source_url"],
        "address_text": clean_address_text(raw["address"], raw["ward"], resolved_district_name, raw["city"], raw["source_url"]),
        "price_million_vnd": price_million,
        "area_sqm": area_sqm_val,
        "price_per_sqm_million": _price_per_sqm_million(price_million, area_sqm_val),
        "first_seen_at": crawled_at,
        "last_seen_at": crawled_at,
        "is_active": True,
    }

    # Validate address_text for OCR/truncation errors - AFTER clean_address_text creates final address
    address_text = fact_row.get("address_text") or ""
    source_url = raw.get("source_url") or ""
    
    # Check both address_text and source URL for OCR/truncation patterns
    ocr_patterns = [
        # Patterns that match Vietnamese diacritics or URL slugs
        (r"(?:Phuong|Phường)\s+Dinh\b", "Phường Dinh (truncation)"),
        (r"(?:Phuong|Phường)\s+Lh\s+E\b", "Phường Lh E (OCR - malformed ward)"),
        (r"(?:Phuong|Phường)\s+Thanh\s+Cong\s+The\b", "Phường Thanh Cong The (OCR - truncation)"),
        (r"\bLh\s+E\b", "Lh E (OCR - ward fragment)"),
        # Also check URL for URL-slugified OCR patterns
        (r"lh-e", "lh-e in URL (OCR - malformed ward slug)"),
        (r"thanh-cong-the", "thanh-cong-the in URL (OCR - truncation slug)"),
        # User-reported lỗi từ crawlers
        (r"(?:Phuong|Phường)\s+Chi\b", "Phường Chi (truncation - Đan Phượng)"),
        (r"(?:Phuong|Phường)\s+Vien\b", "Phường Vien (truncation - Hoài Đức)"),
        (r"Quan\s+Hoang\s+Mai\b", "Quận Hoàng Mai (incomplete address - missing ward)"),
    ]

    for pattern, reason in ocr_patterns:
        # Check both address_text and URL
        if re.search(pattern, address_text, re.IGNORECASE) or re.search(pattern, source_url, re.IGNORECASE):
            return None, {
                "source_name": source,
                "listing_url": raw["source_url"],
                "error_stage": "transform",
                "error_code": "ADDRESS_ERROR",
                "error_message": f"Địa chỉ chứa lỗi OCR/truncation: {reason}",
                "raw_payload": raw["raw_payload"],
            }

    return fact_row, None


# Upsert / Insert / Prune

def upsert_fact_rows(pg: PostgreSQLConnect, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    # Determine which optional columns exist in the target DB
    pg.cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",
        ("fact_property_listing",),
    )
    existing_cols = {r[0] for r in pg.cursor.fetchall()}

    columns = ["run_id", "source_id", "external_id", 
        "district_id", "type_id", "date_key",
        "price_band_id", "area_band_id", "title", "listing_url",
        "address_text", "price_million_vnd", "area_sqm",
        "price_per_sqm_million", "first_seen_at", "last_seen_at", "is_active",
    ]

    values = [tuple(row.get(col) for col in columns) for row in rows]

    # Inspect UNIQUE constraints to choose a safe ON CONFLICT target
    pg.cursor.execute(
        """
        SELECT tc.constraint_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public'
          AND tc.table_name = %s
          AND tc.constraint_type = 'UNIQUE'
        """,
        ("fact_property_listing",),
    )
    constraints: Dict[str, List[str]] = {}
    for cname, col in pg.cursor.fetchall():
        constraints.setdefault(cname, []).append(col)

    conflict_target: Optional[str] = None
    for cols in constraints.values():
        cols_norm = [c.lower() for c in cols]
        cols_set = set(cols_norm)
        if cols_set == {"source_id", "external_id"}:
            conflict_target = "(source_id, external_id)"
            break
        if cols_set == {"source_id", "listing_url"}:
            conflict_target = "(source_id, listing_url)"
            break

    if conflict_target:
        update_items = [
            "run_id = EXCLUDED.run_id",
            "district_id = EXCLUDED.district_id",
            "type_id = EXCLUDED.type_id",
            "date_key = EXCLUDED.date_key",
            "price_band_id = EXCLUDED.price_band_id",
            "area_band_id = EXCLUDED.area_band_id",
            "title = EXCLUDED.title",
            "listing_url = EXCLUDED.listing_url",
            "address_text = EXCLUDED.address_text",
            "price_million_vnd = EXCLUDED.price_million_vnd",
            "area_sqm = EXCLUDED.area_sqm",
            "price_per_sqm_million = EXCLUDED.price_per_sqm_million",
            "last_seen_at = GREATEST(fact_property_listing.last_seen_at, EXCLUDED.last_seen_at)",
            "is_active = EXCLUDED.is_active",
            "updated_at = CURRENT_TIMESTAMP",
        ]
        update_items.insert(8, "external_id = EXCLUDED.external_id")

        insert_sql = sql.SQL(
            """
            INSERT INTO fact_property_listing ({fields})
            VALUES %s
            ON CONFLICT {target}
            DO UPDATE SET {updates}
            """
        ).format(
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            target=sql.SQL(conflict_target),
            updates=sql.SQL(", ").join(sql.SQL(i) for i in update_items),
        )
    else:
        insert_sql = sql.SQL(
            """
            INSERT INTO fact_property_listing ({fields})
            VALUES %s
            ON CONFLICT DO NOTHING
            """
        ).format(fields=sql.SQL(", ").join(map(sql.Identifier, columns)))

    execute_values(pg.cursor, insert_sql.as_string(pg.connection), values)
    return len(values)


def insert_quarantine_rows(pg: PostgreSQLConnect, run_id: str, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    payloads = []
    for row in rows:
        payloads.append((
            run_id,
            row.get("source_name"),
            row.get("listing_url"),
            row.get("error_stage"),
            row.get("error_code"),
            row.get("error_message"),
            Json(row.get("raw_payload") or {}),
        ))
    execute_values(
        pg.cursor,
        """
        INSERT INTO quarantine_listing (
            run_id, source_name, listing_url,
            error_stage, error_code, error_message, raw_payload
        ) VALUES %s
        """,
        payloads,
    )
    return len(payloads)


def backfill_fact_address_text(pg: PostgreSQLConnect) -> int:
    """Rebuild address_text cho các row cũ để sửa lại địa chỉ thô."""
    pg.cursor.execute(
        """
        SELECT f.listing_id, f.address_text, f.listing_url, d.district_name
        FROM fact_property_listing f
        LEFT JOIN dim_district d ON d.district_id = f.district_id
        """
    )
    rows = pg.cursor.fetchall()
    updates: List[Tuple[str, int]] = []
    for listing_id, address_text, listing_url, district_name in rows:
        rebuilt = clean_address_text(address_text, None, district_name, "Hà Nội", listing_url)
        if rebuilt and rebuilt != (address_text or ""):
            updates.append((rebuilt, listing_id))
    for rebuilt, listing_id in updates:
        pg.cursor.execute(
            "UPDATE fact_property_listing SET address_text = %s, updated_at = CURRENT_TIMESTAMP WHERE listing_id = %s",
            (rebuilt, listing_id),
        )
    return len(updates)


def prune_stale_source_rows(
    pg: PostgreSQLConnect,
    source_id: int,
    keep_listing_urls: Sequence[str],
    records_read: int,
) -> int:
    """Xóa các tin cũ không còn trong lần crawl mới nhất.

    Guard: chỉ prune khi crawl thực sự có dữ liệu đáng kể,
    tránh xóa toàn bộ khi crawl bị lỗi/partial.
    """
    if records_read <= 0:
        logger.warning("Bỏ qua prune: records_read=0, crawl có thể đã lỗi")
        return 0

    load_ratio = len(keep_listing_urls) / records_read if records_read > 0 else 0
    if load_ratio < 0.1:
        logger.warning(
            "Bỏ qua prune: load_ratio=%.2f%% quá thấp (loaded=%d, read=%d)",
            load_ratio * 100, len(keep_listing_urls), records_read,
        )
        return 0

    if not keep_listing_urls:
        return 0

    pg.cursor.execute(
        """
        DELETE FROM fact_property_listing
        WHERE source_id = %s
          AND NOT (listing_url = ANY(%s))
        """,
        (source_id, list(keep_listing_urls)),
    )
    return pg.cursor.rowcount