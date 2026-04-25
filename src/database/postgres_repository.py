"""ETL helpers để chuyển raw listings từ MongoDB sang PostgreSQL."""

from __future__ import annotations

import json
import sys
import re
import math
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from psycopg2 import sql
from psycopg2.extras import Json, execute_values


# Cho phép chạy trực tiếp file: python src/database/postgres_repository.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database.mongodb_connect import MongoDBConnect
from src.database.postgres_connect import PostgreSQLConnect


PROPERTY_TYPE_MAP = {
    "Căn hộ/Chung cư": "chung_cu",
    "Nhà biệt thự/Liền kề": "biet_thu",
    "Nhà mặt phố/Shophouse": "nha_mat_pho",
    "Nhà riêng/Nhà ngõ hẻm": "nha_rieng",
    "Đất nền/Đất thổ cư": "dat_nen",
    "Phòng trọ": "nha_rieng",
    "Mặt bằng kinh doanh": "nha_mat_pho",
    "Kho bãi/Nhà xưởng": "nha_rieng",
    "Khác": "khac",
}

STREET_ACCENT_MAP = {
    "le quang dao": "Lê Quang Đạo",
    "to huu": "Tố Hữu",
    "co loa": "Cổ Loa",
    "nguyen van huyen": "Nguyễn Văn Huyên",
    "tay thang long": "Tây Thăng Long",
    "trau quy": "Trâu Quỳ",
    "phuc loi": "Phúc Lợi",
    "giap bat": "Giáp Bát",
    "phap van": "Pháp Vân",
    "nguyen van": "Nguyễn Văn",
    "xuan dinh": "Xuân Đỉnh",
    "truong sa": "Trường Sa",
    "phu thuong": "Phú Thượng",
    "phu do": "Phú Đô",
    "phu luong": "Phú Lương",
    "duong noi": "Dương Nội",
    "o dien": "Ô Diên",
    "dai mo": "Đại Mỗ",
    "viet hung": "Việt Hưng",
    "bat khoi": "Bát Khối",
    "duong xa": "Dương Xá",
    "dong hoi": "Đông Hội",
    "lac long": "Lạc Long",
    "quoc lo 32": "Quốc lộ 32",
    "ngoc trai 3": "Ngọc Trai 3",
    "ngoc trai 6": "Ngọc Trai 6",
    "thang long": "Thăng Long",
    "thai thinh": "Thái Thịnh",
    "tran dai nghia": "Trần Đại Nghĩa",
    "nguyen mau tai": "Nguyễn Mậu Tài",
    "nguyet que": "Nguyệt Quế",
    "duc giang": "Đức Giang",
    "thuong mai": "Thương Mại",
    "nguyen huu tho": "Nguyễn Hữu Thọ",
    "nguyen luong bang": "Nguyễn Lương Bằng",
    "nguyen van linh": "Nguyễn Văn Linh",
    "chua lang": "Chùa Láng",
    "pho chua lang": "Chùa Láng",
    "thuong mai pho cau coc": "Thương Mại - Cầu Cốc",
    "cau coc": "Cầu Cốc",
    "hoi duc": "Hoài Đức",
    "thach hoa": "Thạch Hòa",
    "tan mai": "Tân Mai",
    "thien hien": "Thiên Hiền",
    "ngoc hoi": "Ngọc Hồi",
    "duong dai ang": "Đại Áng",
    "cau giay": "Cầu Giấy",
    "duong xa vinhomes ocean park gia lam": "Vinhomes Ocean Park Gia Lâm",
    "tan hoi": "Tân Hội",
    "vinhomes wonder city": "Vinhomes Wonder City",
    "noble palace tay thang long": "Noble Palace Tây Thăng Long",
}

ADMIN_ACCENT_MAP = {
    "tay ho": "Tây Hồ",
    "nam tu liem": "Nam Từ Liêm",
    "bac tu liem": "Bắc Từ Liêm",
    "ha dong": "Hà Đông",
    "dong anh": "Đông Anh",
    "dan phuong": "Đan Phượng",
    "hoai duc": "Hoài Đức",
    "gia lam": "Gia Lâm",
    "cau giay": "Cầu Giấy",
    "dong da": "Đống Đa",
    "tan lap": "Tân Lập",
    "tan hoi": "Tân Hội",
    "dong hoi": "Đông Hội",
    "phu thuong": "Phú Thượng",
    "tay mo": "Tây Mỗ",
    "trung van": "Trung Văn",
    "xuan la": "Xuân La",
    "duc thuong": "Đức Thượng",
    "di trach": "Di Trạch",
    "trau quy": "Trâu Quỳ",
    "thach ban": "Thạch Bàn",
    "nghia do": "Nghĩa Đô",
    "nam dong": "Nam Đồng",
    "trung liet": "Trung Liệt",
    "hong ha": "Hồng Hà",
    "o dien": "Ô Diên",
}


ADDRESS_PREFIX_MAP = [
    (r"\bDuong\b", "Đường"),
    (r"\bPho\b", "Phố"),
    (r"\bNgo\b", "Ngõ"),
    (r"\bQuan\b", "Quận"),
    (r"\bHuyen\b", "Huyện"),
    (r"\bPhuong\b", "Phường"),
    (r"\bXa\b", "Xã"),
    (r"\bThi xa\b", "Thị xã"),
    (r"\bQuoc lo\b", "Quốc lộ"),
    (r"\bTinh lo\b", "Tỉnh lộ"),
]


@dataclass
class LoadStats:
    records_read: int = 0
    records_loaded: int = 0
    records_quarantined: int = 0


def _normalize_text(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = normalized.replace("đ", "d")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _clean_location_name(value: Optional[object]) -> Optional[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return None

    normalized = re.sub(r"^(quan|huyen|thi xa|phuong|xa|thi tran)\s+", "", normalized)
    normalized = re.sub(r"^(q|h|p|x|tx)\s+", "", normalized)
    normalized = re.sub(r"\b(tp|thanh pho|thanh pho ha noi|ha noi)\b", "", normalized).strip()
    normalized = re.sub(r"\bmoi$", "", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _title_case_from_normalized(normalized: str) -> str:
    return " ".join(part.capitalize() for part in normalized.split())


def _unique_texts(values: Iterable[Optional[object]]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _district_alias_names(district_name: str, district_type: Optional[str]) -> List[str]:
    aliases: List[str] = [district_name, district_name.lower()]
    if district_type == "quan":
        aliases.extend([
            f"Quận {district_name}",
            f"Q. {district_name}",
            f"Q.{district_name}",
            f"Quan {district_name}",
        ])
    elif district_type == "huyen":
        aliases.extend([
            f"Huyện {district_name}",
            f"H. {district_name}",
            f"H.{district_name}",
            f"Huyen {district_name}",
        ])
    elif district_type == "thi_xa":
        aliases.extend([
            f"Thị xã {district_name}",
            f"TX. {district_name}",
            f"TX {district_name}",
            f"Thi xa {district_name}",
        ])
    return _unique_texts(aliases)


def _ward_alias_names(ward_name: str, canonical_name: str) -> List[str]:
    aliases: List[str] = [ward_name, canonical_name, canonical_name.lower()]
    cleaned = _clean_location_name(ward_name)
    if cleaned:
        aliases.append(_title_case_from_normalized(cleaned))
    return _unique_texts(aliases)


def _accentize_street_name(normalized_street: str) -> str:
    key = _normalize_text(normalized_street) or ""
    if key in STREET_ACCENT_MAP:
        return STREET_ACCENT_MAP[key]
    for alias in sorted(STREET_ACCENT_MAP.keys(), key=len, reverse=True):
        if key == alias or key.startswith(f"{alias} ") or key.endswith(f" {alias}"):
            return STREET_ACCENT_MAP[alias]
    return _title_case_from_normalized(key) if key else normalized_street


def _accentize_phrase_exact(raw_text: str) -> str:
    key = _normalize_text(raw_text) or ""
    if key in STREET_ACCENT_MAP:
        return STREET_ACCENT_MAP[key]
    return _title_case_from_normalized(key) if key else raw_text


def _accentize_admin_name(raw_text: Optional[object]) -> Optional[str]:
    if raw_text is None:
        return None
    text = str(raw_text).strip()
    if not text:
        return None

    prefix = ""
    rest = text
    for p in ("Quận", "Huyện", "Thị xã", "Thị trấn", "Phường", "Xã"):
        if re.match(rf"^{p}\b", text, flags=re.IGNORECASE):
            prefix = p
            rest = re.sub(rf"^{p}\s+", "", text, flags=re.IGNORECASE).strip()
            break

    key = _normalize_text(rest)
    if not key:
        return text

    # Old-admin suffix fragments like "Phu Thuong Moi" should map to canonical names.
    if key.endswith(" moi"):
        key = key[:-4].strip()
        if not key:
            return text

    pretty = ADMIN_ACCENT_MAP.get(key) or STREET_ACCENT_MAP.get(key) or _title_case_from_normalized(key)
    return f"{prefix} {pretty}".strip() if prefix else pretty


def _accentize_known_segment(segment: str) -> str:
    text = segment.strip()
    if not text:
        return text

    # Admin segment: Quận/Huyện/Phường/Xã ...
    if re.match(r"^(Quận|Huyện|Thị xã|Thị trấn|Phường|Xã)\b", text, flags=re.IGNORECASE):
        return _accentize_admin_name(text) or text

    key = _normalize_text(text)
    if not key:
        return text
    if key in ADMIN_ACCENT_MAP:
        return ADMIN_ACCENT_MAP[key]
    if key in STREET_ACCENT_MAP:
        return STREET_ACCENT_MAP[key]
    return text


def _normalize_address_parts(text: str) -> str:
    raw_parts = [p.strip() for p in text.split(",") if p and p.strip()]
    if not raw_parts:
        return text

    parts = [_accentize_known_segment(p) for p in raw_parts]
    normalized_parts = [(_normalize_text(p) or "") for p in parts]
    has_explicit_ward = any(re.search(r"\b(phuong|xa|thi tran)\b", n) for n in normalized_parts)

    kept: List[str] = []
    kept_norm: set[str] = set()
    for part in parts:
        n = _normalize_text(part) or ""
        if not n:
            continue

        # Drop old-admin noise fragments like "O Dien Moi", "Phu Thuong Moi", "Dong Da Moi".
        if re.search(r"\bmoi\b", n):
            is_admin_moi = bool(re.search(r"\b(phuong|xa|thi tran|quan|huyen|thi xa)\b", n))
            is_short_alias_moi = (not is_admin_moi) and n.endswith(" moi") and len(n.split()) <= 4
            if is_admin_moi or (is_short_alias_moi and has_explicit_ward):
                continue

        if n in kept_norm:
            continue
        kept_norm.add(n)
        kept.append(part)

    return ", ".join(kept)


def _accentize_address_prefixes(text: str) -> str:
    result = text
    for pattern, replacement in ADDRESS_PREFIX_MAP:
        result = re.sub(
            rf"(^|[,(]\s*){pattern}\b",
            lambda match: f"{match.group(1)}{replacement}",
            result,
            flags=re.IGNORECASE,
        )

    result = re.sub(r"^(Phố|Đường|Ngõ|Quận|Huyện|Phường|Xã|Thị xã)\s+\1\s+", r"\1 ", result, flags=re.IGNORECASE)
    result = re.sub(r"^(Phố|Đường|Ngõ)\s+(Phố|Đường|Ngõ)\s+", r"\1 ", result, flags=re.IGNORECASE)
    result = re.sub(r"\b(Phố|Đường|Ngõ)\s+([A-Za-zÀ-ỹ]+)\s+\1\b", r"\1 \2", result, flags=re.IGNORECASE)
    return result


def _strip_legacy_parenthetical_parts(text: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        content = match.group(1)
        normalized = _normalize_text(content) or ""
        # Drop old-admin fragments like: (Phường ..., Hà Nội mới), (Xã ..., Hà Nội mới)
        if "ha noi moi" in normalized:
            return ""
        if re.search(r"\b(phuong|xa|thi tran|quan|huyen)\b", normalized) and re.search(r"\bmoi\b", normalized):
            return ""
        return match.group(0)

    cleaned = re.sub(r"\(([^)]*)\)", _replace, text)
    # Handle malformed tail like: "..., (Phường ..., Hà Nội mới" (missing closing parenthesis)
    open_idx = cleaned.rfind("(")
    if open_idx != -1 and ")" not in cleaned[open_idx:]:
        tail = cleaned[open_idx + 1 :]
        if "ha noi moi" in (_normalize_text(tail) or ""):
            cleaned = cleaned[:open_idx].rstrip(" ,;")
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


def _infer_district_type(raw_district: Optional[object]) -> Optional[str]:
    text = _normalize_text(raw_district)
    if not text:
        return None
    if text.startswith("huyen ") or text.startswith("h "):
        return "huyen"
    if text.startswith("thi xa") or text.startswith("tx "):
        return "thi_xa"
    if text.startswith("quan ") or text.startswith("q "):
        return "quan"
    return None


def _extract_street_from_listing_url(listing_url: Optional[object]) -> Optional[str]:
    if listing_url is None:
        return None

    text = str(listing_url).strip()
    if not text:
        return None

    try:
        path = urlparse(text).path
        if not path:
            return None
        first_segment = path.strip("/").split("/")[0]
    except Exception:
        return None

    if not first_segment:
        return None

    patterns = [
        (r"-duong-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-quan-|-huyen-|-khu-|-pr|$)", "Đường"),
        (r"-pho-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-quan-|-huyen-|-khu-|-pr|$)", "Phố"),
        (r"-ngo-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-quan-|-huyen-|-khu-|-pr|$)", "Ngõ"),
        (r"-quoc-lo-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-quan-|-huyen-|-khu-|-pr|$)", "Quốc lộ"),
        (r"-tinh-lo-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-quan-|-huyen-|-khu-|-pr|$)", "Tỉnh lộ"),
    ]

    for pattern, prefix in patterns:
        match = re.search(pattern, first_segment)
        if not match:
            continue
        raw_name = match.group(1).strip("-")
        if not raw_name:
            continue
        if raw_name.startswith(("xa-", "phuong-", "quan-", "huyen-", "thi-tran-")):
            continue
        if raw_name.startswith("pho-"):
            raw_name = raw_name[4:]
        pretty_name = _accentize_street_name(raw_name.replace("-", " "))
        pretty_name = re.sub(r"\s+", " ", pretty_name).strip()
        if pretty_name:
            return f"{prefix} {pretty_name}"

    return None


def _extract_project_from_listing_url(listing_url: Optional[object]) -> Optional[str]:
    """Trích tên dự án từ URL tin đăng BDS (vd: vinhomes-wonder-city từ URL slug)."""
    if listing_url is None:
        return None
    text = str(listing_url).strip()
    if not text:
        return None
    try:
        path = urlparse(text).path
        if not path:
            return None
        first_segment = path.strip("/").split("/")[0]
    except Exception:
        return None
    if not first_segment:
        return None

    slug = re.sub(r"\.html?$", "", first_segment, flags=re.IGNORECASE)

    # Pattern thường gặp trên BDS: ...-{num}-{project-name}-pr123456(.htm)
    match = re.search(r"-(\d+)-([a-z][a-z0-9-]*[a-z])-pr\d+$", slug)
    if not match:
        # Pattern cũ: ...-{num}-{project-name}-{project_id}
        match = re.search(r"-(\d+)-([a-z][a-z0-9-]*[a-z])-(\d+)$", slug)
    if match:
        # Nhóm project-name có index khác nhau giữa 2 regex trên.
        project_slug = match.group(2) if len(match.groups()) >= 2 else match.group(1)

        # Một số slug chứa tiền tố hành chính trước tên dự án (vd: xa-duc-thuong-dong-duong-residence).
        # Loại phần tiền tố để giữ lại tên dự án gốc.
        tokens = [t for t in project_slug.split("-") if t]
        if tokens:
            if tokens[0] in {"xa", "phuong", "quan", "huyen"} and len(tokens) > 3:
                tokens = tokens[3:]
            elif len(tokens) > 3 and tokens[0] == "thi" and tokens[1] == "tran":
                tokens = tokens[3:]
            elif tokens[0] in {"tx", "tp"} and len(tokens) > 2:
                tokens = tokens[2:]
            project_slug = "-".join(tokens)

        # Chỉ trả về nếu tên dự án có ít nhất 2 từ (tránh false positive)
        if "-" in project_slug:
            pretty = _accentize_phrase_exact(project_slug.replace("-", " "))
            return pretty if pretty else None
    return None


def _extract_admin_location_from_listing_url(listing_url: Optional[object]) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {"ward": None, "district": None}
    if listing_url is None:
        return result

    text = str(listing_url).strip()
    if not text:
        return result

    try:
        path = urlparse(text).path
        if not path:
            return result
        slug = path.strip("/").split("/")[0]
    except Exception:
        return result

    if not slug:
        return result

    slug = re.sub(r"\.html?$", "", slug, flags=re.IGNORECASE)

    def _sanitize_admin_slug_name(raw_name: str, max_tokens: int = 3) -> str:
        tokens = [t for t in raw_name.split("-") if t]
        if not tokens:
            return raw_name

        stop_tokens = {
            "khu", "du", "du-an", "vinhomes", "residence", "city", "garden", "villas", "global",
            "smart", "park", "by", "gia", "kita", "wonder", "palace", "apartment", "tower",
        }
        kept: List[str] = []
        for token in tokens:
            if token.isdigit() and len(kept) >= 1:
                break
            if token in stop_tokens and len(kept) >= 1:
                break
            kept.append(token)

        if not kept:
            kept = tokens

        # Ward/district names in Hanoi are usually short; prevent over-capturing project suffixes.
        if len(kept) > max_tokens:
            kept = kept[:max_tokens]

        return "-".join(kept)

    def _extract(pattern: str, prefix: str, max_tokens: int = 3) -> Optional[str]:
        match = re.search(pattern, slug)
        if not match:
            return None
        raw_name = match.group(1).strip("-")
        if not raw_name:
            return None
        raw_name = _sanitize_admin_slug_name(raw_name, max_tokens=max_tokens)
        pretty = _accentize_admin_name(raw_name.replace("-", " ")) or _accentize_street_name(raw_name.replace("-", " "))
        pretty = re.sub(r"\s+", " ", pretty).strip()
        return f"{prefix} {pretty}" if pretty else None

    # Ward-level tokens.
    result["ward"] = (
        _extract(r"-phuong-([a-z0-9-]+?)(?:-quan-|-huyen-|-thi-xa-|-pr|$)", "Phường", max_tokens=2)
        or _extract(r"-xa-([a-z0-9-]+?)(?:-quan-|-huyen-|-thi-xa-|-pr|$)", "Xã", max_tokens=2)
        or _extract(r"-thi-tran-([a-z0-9-]+?)(?:-quan-|-huyen-|-thi-xa-|-pr|$)", "Thị trấn", max_tokens=2)
    )

    # District-level tokens.
    result["district"] = (
        _extract(r"-quan-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-pr|$)", "Quận")
        or _extract(r"-huyen-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-pr|$)", "Huyện")
        or _extract(r"-thi-xa-([a-z0-9-]+?)(?:-phuong-|-xa-|-thi-tran-|-pr|$)", "Thị xã")
    )

    return result


def _contains_normalized(haystack: str, needle: Optional[object]) -> bool:
    if needle is None:
        return False
    n_key = _normalize_text(needle)
    h_key = _normalize_text(haystack) or ""
    return bool(n_key and n_key in h_key)


def _has_ward_token(text: str) -> bool:
    return bool(re.search(r"\b(Phường|Xã|Thị trấn)\b", text, flags=re.IGNORECASE))


def _has_district_token(text: str) -> bool:
    return bool(re.search(r"\b(Quận|Huyện|Thị xã)\b", text, flags=re.IGNORECASE))


def _clean_address_text(
    address: Optional[object],
    ward: Optional[object],
    district: Optional[object],
    city: Optional[object],
    listing_url: Optional[object] = None,
) -> Optional[str]:
    """Làm sạch address_text để lưu vào fact_property_listing.

    Nguyên tắc cốt lõi:
    - KHÔNG bao giờ dùng _normalize_text() output để hiển thị — chỉ dùng để so sánh/lookup.
    - Giữ nguyên dấu từ chuỗi gốc (MongoDB đã có dấu từ crawler).
    - Chỉ bổ sung ward/district khi thực sự thiếu, dùng dấu gốc từ MongoDB.
    - Không đoán tên dự án từ URL — crawler phải lấy đúng từ trang detail.
    """
    # -----------------------------------------------------------------
    # 1. Chuẩn bị chuỗi text gốc — giữ nguyên dấu
    # -----------------------------------------------------------------
    text = str(address).strip() if address is not None else ""

    # Chuỗi dùng để LOOKUP từ URL (không hiển thị)
    url_loc = _extract_admin_location_from_listing_url(listing_url)

    if text:
        # Flatten multiline, xóa ký tự rác đầu chuỗi
        text = re.sub(r"[\r\n]+", ", ", text)
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"^[\s\.,;:\-_|/\\•·]+", "", text).strip()

        # Mở rộng viết tắt prefix — dùng replace trực tiếp, không qua normalize
        text = re.sub(r"\bQ\.\s*", "Quận ", text)
        text = re.sub(r"\bH\.\s*", "Huyện ", text)
        text = re.sub(r"\bP\.\s*", "Phường ", text)
        text = re.sub(r"\bX\.\s*", "Xã ", text)
        text = re.sub(r"\bTX\.\s*", "Thị xã ", text)

        # Bỏ chú thích địa chỉ mới sau sáp nhập: (Xã Ô Diên, Hà Nội mới)
        text = _strip_legacy_parenthetical_parts(text)
        text = re.sub(r"\b(?:Ha\s*Noi|Hà\s*Nội)\s*mới\b", "Hà Nội", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()

        # -----------------------------------------------------------------
        # 2. Bổ sung ward/district NẾU thiếu — dùng chuỗi gốc có dấu
        #    KHÔNG dùng _accentize_admin_name() vì có thể mất dấu khi key
        #    không có trong ADMIN_ACCENT_MAP
        # -----------------------------------------------------------------
        # Ward: ưu tiên giá trị gốc từ MongoDB (đã có dấu), fallback URL
        ward_raw = str(ward).strip() if ward else None
        ward_candidate: Optional[str] = ward_raw or url_loc.get("ward")

        # District: ưu tiên giá trị gốc từ MongoDB, fallback URL
        district_raw = str(district).strip() if district else None
        district_candidate: Optional[str] = district_raw or url_loc.get("district")

        # Bổ sung ward chỉ khi chuỗi chưa có token Phường/Xã
        if ward_candidate and not _has_ward_token(text) and not _contains_normalized(text, ward_candidate):
            text = f"{text}, {ward_candidate}"

        # Bổ sung district chỉ khi chuỗi chưa có token Quận/Huyện
        if district_candidate and not _has_district_token(text) and not _contains_normalized(text, district_candidate):
            text = f"{text}, {district_candidate}"

        # -----------------------------------------------------------------
        # 3. Đảm bảo luôn kết thúc bằng "Hà Nội"
        # -----------------------------------------------------------------
        parts = [p.strip() for p in text.split(",") if p and p.strip()]
        # Bỏ phần "ha noi moi" nếu còn sót
        parts = [p for p in parts if (_normalize_text(p) or "") not in ("ha noi moi", "ha noi moi hanoi")]
        non_city = [p for p in parts if (_normalize_text(p) or "") not in ("ha noi", "hanoi")]
        parts = non_city + ["Hà Nội"]
        # Bỏ trùng nhau (so sánh normalized, giữ chuỗi gốc có dấu)
        seen_norm: set = set()
        deduped = []
        for p in parts:
            key = _normalize_text(p) or p
            if key not in seen_norm:
                seen_norm.add(key)
                deduped.append(p)
        text = ", ".join(deduped)
        text = re.sub(r"\s+", " ", text).strip()

        if text:
            return text

    # -----------------------------------------------------------------
    # 4. Fallback khi address rỗng — ghép từ ward/district/city gốc
    # -----------------------------------------------------------------
    fallback_parts: List[str] = []
    ward_raw = str(ward).strip() if ward else None
    district_raw = str(district).strip() if district else None

    for part in [ward_raw, url_loc.get("ward"), district_raw, url_loc.get("district"), city]:
        if not part:
            continue
        token = re.sub(r"^[\s\.,;:\-_|/\\•·]+", "", str(part).strip()).strip()
        key = _normalize_text(token) or ""
        if token and key not in ("ha noi moi",):
            fallback_parts.append(token)

    if not fallback_parts:
        return None

    non_city = [p for p in fallback_parts if (_normalize_text(p) or "") not in ("ha noi", "hanoi")]
    seen_norm2: set = set()
    deduped2 = []
    for p in non_city:
        key = _normalize_text(p) or p
        if key not in seen_norm2:
            seen_norm2.add(key)
            deduped2.append(p)
    return ", ".join(deduped2 + ["Hà Nội"])


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


def _extract_raw_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
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


def _lookup_map(cursor, query: str) -> Dict[str, Any]:
    cursor.execute(query)
    rows = cursor.fetchall()
    mapping: Dict[str, Any] = {}
    for row in rows:
        key = _normalize_text(row[0])
        if key:
            mapping[key] = row[1]
    return mapping


def _run_sql_file(cursor, file_path: Path) -> None:
    sql_text = file_path.read_text(encoding="utf-8")
    cursor.execute(sql_text)


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
    return cursor.fetchone()[0] is not None


def _column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    return cursor.fetchone() is not None


def _constraint_exists(cursor, table_name: str, constraint_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = 'public'
            AND table_name = %s
            AND constraint_name = %s
        """,
        (table_name, constraint_name),
    )
    return cursor.fetchone() is not None


def _drop_incompatible_table(cursor, table_name: str, required_columns: set[str]) -> None:
    if not _table_exists(cursor, table_name):
        return

    missing_cols = [col for col in required_columns if not _column_exists(cursor, table_name, col)]
    if missing_cols:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")


def _ensure_existing_schema_compatibility(pg: PostgreSQLConnect) -> None:
    cursor = pg.cursor
    _drop_incompatible_table(
        cursor,
        "dim_ward",
        required_columns={"ward_id", "district_id", "ward_name", "city_name", "created_at"},
    )
    _drop_incompatible_table(
        cursor,
        "fact_property_listing",
        required_columns={"source_id", "district_id", "type_id", "date_key", "listing_url"},
    )


def _ensure_alias_schema(pg: PostgreSQLConnect) -> None:
    cursor = pg.cursor

    if _table_exists(cursor, "dim_district"):
        if not _column_exists(cursor, "dim_district", "alias_names"):
            cursor.execute("ALTER TABLE dim_district ADD COLUMN alias_names TEXT[] NOT NULL DEFAULT '{}'::text[]")

        cursor.execute("SELECT district_id, district_name, district_type FROM dim_district")
        for district_id, district_name, district_type in cursor.fetchall():
            aliases = _district_alias_names(district_name, district_type)
            cursor.execute(
                "UPDATE dim_district SET alias_names = %s WHERE district_id = %s",
                (aliases, district_id),
            )

    if _table_exists(cursor, "dim_ward"):
        if not _column_exists(cursor, "dim_ward", "canonical_name"):
            cursor.execute("ALTER TABLE dim_ward ADD COLUMN canonical_name VARCHAR(120)")
        if not _column_exists(cursor, "dim_ward", "alias_names"):
            cursor.execute("ALTER TABLE dim_ward ADD COLUMN alias_names TEXT[] NOT NULL DEFAULT '{}'::text[]")

        cursor.execute(
            """
            UPDATE dim_ward
            SET canonical_name = COALESCE(NULLIF(canonical_name, ''), ward_name),
                alias_names = COALESCE(alias_names, '{}'::text[])
            """
        )

        cursor.execute("SELECT ward_id, ward_name, canonical_name FROM dim_ward")
        for ward_id, ward_name, canonical_name in cursor.fetchall():
            aliases = _ward_alias_names(ward_name, canonical_name)
            cursor.execute(
                "UPDATE dim_ward SET alias_names = %s WHERE ward_id = %s",
                (aliases, ward_id),
            )

        cursor.execute("ALTER TABLE dim_ward ALTER COLUMN canonical_name SET NOT NULL")
        cursor.execute("ALTER TABLE dim_ward ALTER COLUMN alias_names SET DEFAULT '{}'::text[]")
        cursor.execute("ALTER TABLE dim_ward ALTER COLUMN alias_names SET NOT NULL")
        if _constraint_exists(cursor, "dim_ward", "uq_dim_ward"):
            cursor.execute("ALTER TABLE dim_ward DROP CONSTRAINT uq_dim_ward")
        if not _constraint_exists(cursor, "dim_ward", "uq_dim_ward_canonical"):
            cursor.execute(
                "ALTER TABLE dim_ward ADD CONSTRAINT uq_dim_ward_canonical UNIQUE (district_id, canonical_name)"
            )


def _ensure_postgres_schema(pg: PostgreSQLConnect) -> None:
    cursor = pg.cursor
    _ensure_existing_schema_compatibility(pg)

    sql_dir = PROJECT_ROOT / "sql"
    for sql_file in ["schema.sql", "seed.sql", "trigger.sql"]:
        file_path = sql_dir / sql_file
        if not file_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file SQL: {file_path}")
        if sql_file == "seed.sql":
            _ensure_alias_schema(pg)
        _run_sql_file(cursor, file_path)

    _ensure_alias_schema(pg)

    # ward_id is no longer used in fact table because ward extraction quality is inconsistent.
    if _table_exists(cursor, "fact_property_listing") and _column_exists(cursor, "fact_property_listing", "ward_id"):
        cursor.execute("DROP INDEX IF EXISTS idx_fact_district_ward")
        cursor.execute("ALTER TABLE fact_property_listing DROP COLUMN ward_id")

    cursor.execute(
        """
        INSERT INTO dim_property_type (type_name)
        VALUES ('khac')
        ON CONFLICT (type_name) DO NOTHING
        """
    )


def _load_dim_maps(pg: PostgreSQLConnect) -> Dict[str, Dict[str, Any]]:
    cursor = pg.cursor

    source_map = _lookup_map(
        cursor,
        "SELECT source_name, source_id FROM dim_source",
    )
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
    property_type_map = _lookup_map(
        cursor,
        "SELECT type_name, type_id FROM dim_property_type",
    )
    price_band_map = _lookup_map(
        cursor,
        "SELECT band_name, price_band_id FROM dim_price_band",
    )
    area_band_map = _lookup_map(
        cursor,
        "SELECT band_name, area_band_id FROM dim_area_band",
    )

    return {
        "source": source_map,
        "district": district_map,
        "property_type": property_type_map,
        "price_band": price_band_map,
        "area_band": area_band_map,
    }


def _resolve_price_band(price_million: Optional[float], band_map: Dict[str, Any]) -> Optional[int]:
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
            return band_map.get(_normalize_text(band_name) or "")
    return None


def _resolve_area_band(area_sqm: Optional[float], band_map: Dict[str, Any]) -> Optional[int]:
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
            return band_map.get(_normalize_text(band_name) or "")
    return None


def _resolve_property_type(type_label: Optional[str], property_type_map: Dict[str, Any]) -> Optional[int]:
    mapped_key = PROPERTY_TYPE_MAP.get(type_label or "")
    if not mapped_key:
        return None
    return property_type_map.get(_normalize_text(mapped_key) or "")


def _resolve_source_id(source: str, source_map: Dict[str, Any]) -> Optional[int]:
    return source_map.get(_normalize_text(source) or "")


def _resolve_district_id(district: Optional[object], district_map: Dict[str, Any]) -> Optional[int]:
    key = _clean_location_name(district)
    if not key:
        return None
    return district_map.get(key)


def _build_fact_row(doc: Dict[str, Any], dim_maps: Dict[str, Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    raw = _extract_raw_fields(doc)
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

    source_id = _resolve_source_id(source, dim_maps["source"])
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

    # Giải quyết district_id: thử trực tiếp từ MongoDB field
    district_id = _resolve_district_id(raw["district"], dim_maps["district"])
    resolved_district_name = raw["district"]

    # Fallback tổng quát: khi MongoDB không có district, quét address + URL
    # tìm tên quận/huyện bất kỳ đã seed trong dim_district (hoạt động cho toàn bộ HN)
    if district_id is None:
        # Ghép tất cả text có thể chứa tên quận/huyện
        search_text = _normalize_text(
            " ".join(filter(None, [
                str(raw.get("address", "")),
                str(raw.get("ward", "")),
                str(raw.get("source_url", "")),
            ]))
        )
        if search_text:
            for district_key, d_id in dim_maps["district"].items():
                # district_key đã normalize (vd: "dan phuong", "cau giay")
                if district_key in search_text:
                    district_id = d_id
                    # Tìm tên gốc có dấu từ seed data
                    resolved_district_name = district_key
                    break

    fact_row = {
        "run_id": None,
        "source_id": source_id,
        "district_id": district_id,
        "type_id": _resolve_property_type(raw["property_type"], dim_maps["property_type"]),
        "date_key": date_key,
        "price_band_id": _resolve_price_band(price_million, dim_maps["price_band"]),
        "area_band_id": _resolve_area_band(area_sqm_val, dim_maps["area_band"]),
        "title": raw["title"],
        "listing_url": raw["source_url"],
        "address_text": _clean_address_text(raw["address"], raw["ward"], resolved_district_name, raw["city"], raw["source_url"]),
        "price_million_vnd": price_million,
        "area_sqm": area_sqm_val,
        "price_per_sqm_million": _price_per_sqm_million(price_million, area_sqm_val),
        "first_seen_at": crawled_at,
        "last_seen_at": crawled_at,
        "is_active": True,
    }

    return fact_row, None


def _ensure_run(pg: PostgreSQLConnect, dag_id: str, run_type: str) -> str:
    cursor = pg.cursor
    cursor.execute(
        """
        INSERT INTO etl_run_log (dag_id, run_type, status, started_at)
        VALUES (%s, %s, 'running', CURRENT_TIMESTAMP)
        RETURNING run_id
        """,
        (dag_id, run_type),
    )
    run_id = cursor.fetchone()[0]
    return str(run_id)


def _finish_run(
    pg: PostgreSQLConnect,
    run_id: str,
    status: str,
    records_read: int,
    records_loaded: int,
    records_quarantined: int,
    error_message: Optional[str] = None,
) -> None:
    pg.cursor.execute(
        """
        UPDATE etl_run_log
        SET status = %s,
            ended_at = CURRENT_TIMESTAMP,
            records_read = %s,
            records_loaded = %s,
            records_quarantined = %s,
            error_message = %s
        WHERE run_id = %s
        """,
        (status, records_read, records_loaded, records_quarantined, error_message, run_id),
    )


def _insert_quarantine_rows(pg: PostgreSQLConnect, run_id: str, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    payloads = []
    for row in rows:
        payloads.append(
            (
                run_id,
                row.get("source_name"),
                row.get("listing_url"),
                row.get("error_stage"),
                row.get("error_code"),
                row.get("error_message"),
                Json(row.get("raw_payload") or {}),
            )
        )

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


def _upsert_dim_districts(pg: PostgreSQLConnect, rows: Iterable[Tuple[str, str, Optional[str], List[str]]]) -> None:
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


def _load_raw_docs(source: Optional[str] = None, collection_name: str = "raw_listings") -> List[Dict[str, Any]]:
    with MongoDBConnect.from_env() as mongo:
        db = mongo.db
        query: Dict[str, Any] = {}
        if source:
            query["source"] = source
        cursor = db[collection_name].find(query)
        return list(cursor)


def _prepare_district_seed_rows(docs: Sequence[Dict[str, Any]]) -> List[Tuple[str, str, Optional[str], List[str]]]:
    rows: List[Tuple[str, str, Optional[str], List[str]]] = []
    seen: set[str] = set()
    for doc in docs:
        district_key = _clean_location_name(doc.get("district"))
        if not district_key or district_key in seen:
            continue
        seen.add(district_key)

        # District dimension uses unaccented city label by convention.
        city_name = "Ha Noi"
        district_name = _title_case_from_normalized(district_key)
        district_type = _infer_district_type(doc.get("district"))
        alias_names = _district_alias_names(district_name, district_type)
        rows.append((district_name, city_name, district_type, alias_names))
    return rows


def _upsert_fact_rows(pg: PostgreSQLConnect, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    columns = [
        "run_id",
        "source_id",
        "district_id",
        "type_id",
        "date_key",
        "price_band_id",
        "area_band_id",
        "title",
        "listing_url",
        "address_text",
        "price_million_vnd",
        "area_sqm",
        "price_per_sqm_million",
        "first_seen_at",
        "last_seen_at",
        "is_active",
    ]

    values = [tuple(row.get(col) for col in columns) for row in rows]
    insert_sql = sql.SQL(
        """
        INSERT INTO fact_property_listing ({fields})
        VALUES %s
        ON CONFLICT (source_id, listing_url)
        DO UPDATE SET
            run_id = EXCLUDED.run_id,
            district_id = EXCLUDED.district_id,
            type_id = EXCLUDED.type_id,
            date_key = EXCLUDED.date_key,
            price_band_id = EXCLUDED.price_band_id,
            area_band_id = EXCLUDED.area_band_id,
            title = EXCLUDED.title,
            listing_url = EXCLUDED.listing_url,
            address_text = EXCLUDED.address_text,
            price_million_vnd = EXCLUDED.price_million_vnd,
            area_sqm = EXCLUDED.area_sqm,
            price_per_sqm_million = EXCLUDED.price_per_sqm_million,
            last_seen_at = GREATEST(fact_property_listing.last_seen_at, EXCLUDED.last_seen_at),
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
        """
    ).format(fields=sql.SQL(", ").join(map(sql.Identifier, columns)))

    execute_values(pg.cursor, insert_sql.as_string(pg.connection), values)
    return len(values)


def _backfill_fact_address_text(pg: PostgreSQLConnect) -> int:
    """Rebuild address_text for existing rows to retro-fix old coarse addresses."""
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
        rebuilt = _clean_address_text(address_text, None, district_name, "Hà Nội", listing_url)
        if rebuilt and rebuilt != (address_text or ""):
            updates.append((rebuilt, listing_id))

    for rebuilt, listing_id in updates:
        pg.cursor.execute(
            "UPDATE fact_property_listing SET address_text = %s, updated_at = CURRENT_TIMESTAMP WHERE listing_id = %s",
            (rebuilt, listing_id),
        )
    return len(updates)


def _prune_stale_source_rows(pg: PostgreSQLConnect, source_id: int, keep_listing_urls: Sequence[str]) -> int:
    if not keep_listing_urls:
        pg.cursor.execute(
            "DELETE FROM fact_property_listing WHERE source_id = %s",
            (source_id,),
        )
        return pg.cursor.rowcount

    pg.cursor.execute(
        """
        DELETE FROM fact_property_listing
        WHERE source_id = %s
          AND NOT (listing_url = ANY(%s))
        """,
        (source_id, list(keep_listing_urls)),
    )
    return pg.cursor.rowcount


def debug_address_pipeline(limit: int = 5, source: Optional[str] = None) -> None:
    """Bước 1 — Trace mất dấu qua từng tầng để biết chính xác vấn đề xảy ra ở đâu.

    Chạy:  python -c "from src.database.postgres_repository import debug_address_pipeline; debug_address_pipeline(5)"
    """
    docs = _load_raw_docs(source=source)[:limit]
    print(f"\n{'='*70}")
    print(f"DEBUG ADDRESS PIPELINE — {len(docs)} records")
    print(f"{'='*70}\n")
    for i, doc in enumerate(docs, 1):
        raw = _extract_raw_fields(doc)
        print(f"[{i}] external_id={raw['external_id']}  source={raw['source']}")
        print(f"    MONGO address  : {raw['address']!r}")
        print(f"    MONGO ward     : {raw['ward']!r}")
        print(f"    MONGO district : {raw['district']!r}")
        print(f"    MONGO city     : {raw['city']!r}")
        result = _clean_address_text(
            raw["address"], raw["ward"], raw["district"], raw["city"], raw["source_url"]
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
        return {
            "records_read": 0,
            "records_loaded": 0,
            "records_quarantined": 0,
        }

    with PostgreSQLConnect.from_env() as pg:
        _ensure_postgres_schema(pg)
        run_id = _ensure_run(pg, dag_id=dag_id, run_type=run_type)
        dim_maps = _load_dim_maps(pg)

        source_id_for_prune: Optional[int] = None
        if source:
            source_id_for_prune = _resolve_source_id(source, dim_maps["source"])

        district_seed_rows = _prepare_district_seed_rows(docs)
        _upsert_dim_districts(pg, district_seed_rows)
        dim_maps = _load_dim_maps(pg)

        fact_rows: List[Dict[str, Any]] = []
        quarantine_rows: List[Dict[str, Any]] = []

        for doc in docs:
            fact_row, quarantine_row = _build_fact_row(doc, dim_maps)
            if fact_row is not None:
                fact_row["run_id"] = run_id
                fact_rows.append(fact_row)
            elif quarantine_row is not None:
                quarantine_rows.append(quarantine_row)

        if fact_rows:
            stats.records_loaded = _upsert_fact_rows(pg, fact_rows)

        if source_id_for_prune is not None:
            kept_urls = [str(row.get("listing_url")) for row in fact_rows if row.get("listing_url")]
            _prune_stale_source_rows(pg, source_id_for_prune, kept_urls)

        if quarantine_rows:
            stats.records_quarantined = _insert_quarantine_rows(pg, run_id, quarantine_rows)

        _backfill_fact_address_text(pg)

        status = "partial" if stats.records_quarantined else "success"

        _finish_run(
            pg,
            run_id=run_id,
            status=status,
            records_read=stats.records_read,
            records_loaded=stats.records_loaded,
            records_quarantined=stats.records_quarantined,
        )

    return {
        "records_read": stats.records_read,
        "records_loaded": stats.records_loaded,
        "records_quarantined": stats.records_quarantined,
    }


def load_all_sources_to_postgres(collection_name: str = "raw_listings") -> Dict[str, int]:
    """Load toàn bộ raw listings theo từng source để dễ theo dõi và restart."""
    combined = {"records_read": 0, "records_loaded": 0, "records_quarantined": 0}
    for source in ("chotot", "batdongsan"):
        result = load_raw_listings_to_postgres(source=source, collection_name=collection_name)
        combined["records_read"] += result["records_read"]
        combined["records_loaded"] += result["records_loaded"]
        combined["records_quarantined"] += result["records_quarantined"]
    return combined


if __name__ == "__main__":
    result = load_all_sources_to_postgres()
    print(result)