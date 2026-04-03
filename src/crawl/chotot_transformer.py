"""Biến đổi payload thô của Chợ Tốt thành bản ghi raw đã chuẩn hóa."""

from datetime import UTC, datetime
from typing import Dict, List, Optional

import pandas as pd

from src.crawl.utils.normalizers import normalize_id


def _extract_location_fields(ad: Dict, location: Dict) -> Dict[str, Optional[str]]:
    if not isinstance(location, dict):
        location = {}

    city = ad.get("region_name") or location.get("region_name")
    district = ad.get("area_name") or location.get("area_name")
    ward = location.get("ward_name") or ad.get("ward_name") or ad.get("sub_area_name")
    address = location.get("address") or ad.get("address")

    return {
        "city": city,
        "district": district,
        "ward": ward,
        "address": address,
    }


def _extract_images(ad: Dict) -> List[str]:
    images = ad.get("images") or []
    if not isinstance(images, list):
        images = []

    extracted: List[str] = []
    for image in images:
        if isinstance(image, str) and image:
            extracted.append(image)
        elif isinstance(image, dict):
            image_url = (
                image.get("image_url")
                or image.get("url")
                or image.get("thumbnail")
                or image.get("thumb")
                or image.get("image")
            )
            if image_url:
                extracted.append(image_url)

    if not extracted:
        for fallback_key in ["image", "thumbnail_image", "webp_image"]:
            fallback_value = ad.get(fallback_key)
            if isinstance(fallback_value, str) and fallback_value:
                extracted.append(fallback_value)

    return list(dict.fromkeys(extracted))


def _build_address_text(ad: Dict, location_fields: Dict[str, Optional[str]]) -> Optional[str]:
    address = location_fields.get("address")
    if address:
        return address

    parts = [
        ad.get("street_name"),
        location_fields.get("ward"),
        location_fields.get("district"),
        location_fields.get("city"),
    ]
    cleaned = [str(part).strip() for part in parts if part and str(part).strip()]
    if not cleaned:
        return None

    return ", ".join(cleaned)


def build_detail_record(payload: Dict, ad_id: Optional[str] = None, list_id: Optional[str] = None) -> Optional[Dict]:
    ad = payload.get("ad", {})
    location = ad.get("location") or {}
    location_fields = _extract_location_fields(ad, location)

    ext_id = normalize_id(ad.get("ad_id")) or normalize_id(ad_id) or normalize_id(list_id)
    if not ext_id:
        return None

    return {
        "source": "chotot",
        "external_id": ext_id,
        "source_url": f"https://www.nhatot.com/mua-ban-bat-dong-san/{ext_id}",
        "title": ad.get("subject") or ad.get("title") or "",
        "description": ad.get("body") or "",
        "price_vnd": ad.get("price"),
        "area_sqm": ad.get("area"),
        "address": _build_address_text(ad, location_fields),
        "district": location_fields["district"],
        "ward": location_fields["ward"],
        "city": location_fields["city"],
        "property_type": ad.get("type") or ad.get("category_name"),
        "contact_name": ad.get("account_name") or ad.get("name"),
        "contact_phone": ad.get("phone"),
        "images": _extract_images(ad),
        "raw_payload": payload,
        "crawled_at": datetime.now(UTC).isoformat(),
    }


def build_fallback_record(row: pd.Series) -> Optional[Dict]:
    ext_id = normalize_id(row.get("ad_id"))
    if not ext_id:
        return None

    ad = row.to_dict()
    location_fields = _extract_location_fields(ad, {})

    return {
        "source": "chotot",
        "external_id": ext_id,
        "source_url": f"https://www.nhatot.com/mua-ban-bat-dong-san/{ext_id}",
        "title": row.get("subject") or "",
        "description": "",
        "price_vnd": row.get("price"),
        "area_sqm": row.get("area"),
        "address": _build_address_text(ad, location_fields),
        "district": location_fields["district"],
        "ward": location_fields["ward"],
        "city": location_fields["city"],
        "property_type": None,
        "contact_name": None,
        "contact_phone": None,
        "images": _extract_images(ad),
        "raw_payload": ad,
        "crawled_at": datetime.now(UTC).isoformat(),
    }


