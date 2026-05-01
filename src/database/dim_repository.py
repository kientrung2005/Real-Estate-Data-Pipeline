"""CRUD operations cho dimension tables trong PostgreSQL."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg2.extras import execute_values

from src.database.address_cleaner import (
    clean_location_name,
    district_alias_names,
    infer_district_type,
    normalize_text,
    title_case_from_normalized,
    ward_alias_names,
    _accentize_admin_name,
    ADMIN_ACCENT_MAP,
)
from src.database.postgres_connect import PostgreSQLConnect


PROPERTY_TYPE_MAP = {
    "Căn hộ/Chung cư": "chung_cu",
    "Nhà biệt thự/Liền kề": "biet_thu",
    "Nhà mặt phố/Shophouse": "nha_mat_pho",
    "Nhà riêng/Nhà ngõ hẻm": "nha_rieng",
    "Đất nền/Đất thổ cư": "dat_nen",
    "Phòng trọ": "phong_tro",
    "Mặt bằng kinh doanh": "mat_bang",
    "Kho bãi/Nhà xưởng": "kho_xuong",
    "Khác": "khac",
}




def _lookup_map(cursor, query: str) -> Dict[str, Any]:
    cursor.execute(query)
    rows = cursor.fetchall()
    mapping: Dict[str, Any] = {}
    for row in rows:
        key = normalize_text(row[0])
        if key:
            mapping[key] = row[1]
    return mapping


def load_dim_maps(pg: PostgreSQLConnect) -> Dict[str, Dict[str, Any]]:
    """Load tất cả lookup maps từ dimension tables."""
    cursor = pg.cursor
    source_map = _lookup_map(cursor, "SELECT source_name, source_id FROM dim_source")
    district_map = _lookup_map(
        cursor,
        """
        SELECT lookup_name, district_id
        FROM (
            SELECT district_name AS lookup_name, district_id
            FROM dim_district
            UNION ALL
            SELECT alias_name AS lookup_name, district_id
            FROM dim_district
            CROSS JOIN LATERAL unnest(COALESCE(alias_names, ARRAY[]::text[])) AS alias_name
        ) district_lookup
        """,
    )
    property_type_map = _lookup_map(cursor, "SELECT type_name, type_id FROM dim_property_type")
    price_band_map = _lookup_map(cursor, "SELECT band_name, price_band_id FROM dim_price_band")
    area_band_map = _lookup_map(cursor, "SELECT band_name, area_band_id FROM dim_area_band")
    ward_map = _lookup_map(
        cursor,
        """
        SELECT lookup_name, ward_id
        FROM (
            SELECT ward_name AS lookup_name, ward_id
            FROM dim_ward
            UNION ALL
            SELECT alias_name AS lookup_name, ward_id
            FROM dim_ward
            CROSS JOIN LATERAL unnest(COALESCE(alias_names, ARRAY[]::text[])) AS alias_name
        ) ward_lookup
        """,
    )
    return {
        "source": source_map,
        "district": district_map,
        "property_type": property_type_map,
        "price_band": price_band_map,
        "area_band": area_band_map,
        "ward": ward_map,
    }




def resolve_source_id(source: str, source_map: Dict[str, Any]) -> Optional[int]:
    return source_map.get(normalize_text(source) or "")


def resolve_district_id(district: Optional[object], district_map: Dict[str, Any]) -> Optional[int]:
    key = clean_location_name(district)
    if not key:
        return None
    return district_map.get(key)


def resolve_property_type(type_label: Optional[str], property_type_map: Dict[str, Any]) -> Optional[int]:
    mapped_key = PROPERTY_TYPE_MAP.get(type_label or "")
    if not mapped_key:
        return None
    return property_type_map.get(normalize_text(mapped_key) or "")


def resolve_price_band(price_million: Optional[float], band_map: Dict[str, Any]) -> Optional[int]:
    if price_million is None:
        return None
    candidates = [
        ("duoi_1_ty", 0, 1000),
        ("1_3_ty", 1000, 3000),
        ("3_5_ty", 3000, 5000),
        ("5_10_ty", 5000, 10000),
        ("tren_10_ty", 10000, None),
    ]
    for band_name, min_value, max_value in candidates:
        if price_million >= min_value and (max_value is None or price_million < max_value):
            return band_map.get(normalize_text(band_name) or "")
    return None


def resolve_area_band(area_sqm: Optional[float], band_map: Dict[str, Any]) -> Optional[int]:
    if area_sqm is None:
        return None
    candidates = [
        ("duoi_30m2", 0, 30),
        ("30_50m2", 30, 50),
        ("50_80m2", 50, 80),
        ("80_120m2", 80, 120),
        ("tren_120m2", 120, None),
    ]
    for band_name, min_value, max_value in candidates:
        if area_sqm >= min_value and (max_value is None or area_sqm < max_value):
            return band_map.get(normalize_text(band_name) or "")
    return None


# District seed / upsert

def prepare_district_seed_rows(docs: Sequence[Dict[str, Any]]) -> List[Tuple[str, str, Optional[str], List[str]]]:
    rows: List[Tuple[str, str, Optional[str], List[str]]] = []
    seen: set[str] = set()
    for doc in docs:
        district_key = clean_location_name(doc.get("district"))
        if not district_key or district_key in seen:
            continue
        seen.add(district_key)
        city_name = "Hà Nội"
        # Dùng _accentize_admin_name để có dấu tiếng Việt chuẩn cho tên Quận
        district_name = _accentize_admin_name(district_key) or title_case_from_normalized(district_key)
        d_type = infer_district_type(doc.get("district"))
        aliases = district_alias_names(district_name, d_type)
        rows.append((district_name, city_name, d_type, aliases))
    return rows


def upsert_dim_districts(pg: PostgreSQLConnect, rows: Iterable[Tuple[str, str, Optional[str], List[str]]]) -> None:
    items = [
        (district_name, city_name, district_type, alias_names)
        for district_name, city_name, district_type, alias_names in rows
        if district_name
    ]
    if not items:
        return
    execute_values(
        pg.cursor,
        """
        INSERT INTO dim_district (district_name, city_name, district_type, alias_names)
        VALUES %s
        ON CONFLICT (district_name) DO UPDATE SET
            city_name = EXCLUDED.city_name,
            district_type = COALESCE(EXCLUDED.district_type, dim_district.district_type),
            alias_names = ARRAY(
                SELECT DISTINCT unnest(COALESCE(dim_district.alias_names, '{}'::text[]) || COALESCE(EXCLUDED.alias_names, '{}'::text[]))
            )
        """,
        items,
    )


# Ward upsert logic

def upsert_dim_wards(pg: PostgreSQLConnect, ward_name: str, district_id: int, city_name: str = "Hà Nội") -> int:
    """Tự động thêm phường mới nếu chưa có và trả về ward_id."""
    key = clean_location_name(ward_name)
    if not key:
        return 0
    
    # Chuẩn hóa tên chuẩn (canonical)
    pretty_core = ADMIN_ACCENT_MAP.get(key) or title_case_from_normalized(key)
    
    # Ép tiền tố: Nếu trong ward_name gốc chưa có tiền tố chuẩn, hãy tự thêm vào
    if re.match(r"^(phường|xã|thị trấn|phuong|xa|thi tran)\b", ward_name, flags=re.IGNORECASE):
        # Nếu đã có tiền tố, hãy chuẩn hóa nó
        prefix = "Thị trấn" if "thi tran" in normalize_text(ward_name) else ("Xã" if "xa" in normalize_text(ward_name) else "Phường")
    else:
        # Nếu chưa có tiền tố, hãy suy luận
        communes = {"kiieu ky", "tan hoi", "duong xa", "di trach", "uy no", "xuan non", "da ton", "o dien", "o dien moi", "tan lap", "dong du", "cu khoi", "bat trang", "dong my", "son dong", "nam hong"}
        prefix = "Xã" if key in communes else "Phường"
    
    canonical = f"{prefix} {pretty_core}"
    aliases = ward_alias_names(ward_name, canonical)
    
    pg.cursor.execute(
        """
        INSERT INTO dim_ward (ward_name, district_id, city_name, canonical_name, alias_names)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (district_id, canonical_name) DO UPDATE SET
            ward_name = EXCLUDED.ward_name,
            canonical_name = EXCLUDED.canonical_name,
            alias_names = ARRAY(
                SELECT DISTINCT unnest(COALESCE(dim_ward.alias_names, '{}'::text[]) || COALESCE(EXCLUDED.alias_names, '{}'::text[]))
            )
        RETURNING ward_id
        """,
        (canonical, district_id, city_name, canonical, aliases)
    )
    row = pg.cursor.fetchone()
    return row[0] if row else 0
