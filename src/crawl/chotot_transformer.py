"""Biến đổi payload thô của Chợ Tốt thành bản ghi raw đã chuẩn hóa."""

from datetime import UTC, datetime
from typing import Dict, List, Optional

import pandas as pd

from src.crawl.utils.normalizers import normalize_id


def _normalize_nhatot_url(raw_url: str, ext_id: str) -> str:
    """Đảm bảo URL luôn có .htm và đúng domain."""
    if not raw_url or not raw_url.startswith("http"):
        return f"https://www.nhatot.com/mua-ban-bat-dong-san/{ext_id}.htm"

    # Nếu đã có .htm rồi thì dùng luôn
    if raw_url.endswith(".htm"):
        return raw_url

    # URL thiếu .htm (dạng API listing trả về): thêm vào
    # vd: https://www.nhatot.com/mua-ban-bat-dong-san/175647218
    #  → https://www.nhatot.com/mua-ban-bat-dong-san/175647218.htm
    return raw_url.rstrip("/") + ".htm"


def _resolve_source_url(ad: Dict, list_id: str) -> str:
    """Lấy URL thật từ payload, prioritize list_id vì đây là ID đúng cho URL Nhatot."""
    raw_url = (
        ad.get("share_url")
        or ad.get("url")
        or ad.get("webp_url")
    )
    return _normalize_nhatot_url(raw_url or "", list_id)


def _extract_location_fields(ad: Dict, location: Dict) -> Dict[str, Optional[str]]:
    if not isinstance(location, dict):
        location = {}

    # TODO: hardcode tạm cho Hà Nội, cần dùng region_name nếu mở rộng đa tỉnh
    city = "Hà Nội"
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


def _infer_property_type(ad: Dict) -> Optional[str]:
    text = " ".join(
        [
            str(ad.get("type") or ""),
            str(ad.get("category_name") or ""),
            str(ad.get("subject") or ad.get("title") or ""),
        ]
    ).lower()

    if not text.strip():
        return None

    if any(k in text for k in ["chung cư", "can ho", "căn hộ", "condotel", "penthouse"]):
        return "Căn hộ/Chung cư"
    if any(k in text for k in ["biệt thự", "lien ke", "liền kề", "villa"]):
        return "Nhà biệt thự/Liền kề"
    if any(k in text for k in ["mặt phố", "mat pho", "shophouse", "mặt tiền", "mat tien"]):
        return "Nhà mặt phố/Shophouse"
    if any(k in text for k in ["nhà riêng", "nha rieng", "nhà ngõ", "nha ngo", "hẻm", "hem"]):
        return "Nhà riêng/Nhà ngõ hẻm"
    if any(k in text for k in ["đất", "dat nen", "đất nền", "thổ cư", "tho cu"]):
        return "Đất nền/Đất thổ cư"
    if any(k in text for k in ["phòng trọ", "phong tro", "nhà trọ", "nha tro"]):
        return "Phòng trọ"
    if any(k in text for k in ["mặt bằng", "mat bang", "cửa hàng", "cua hang", "ki ốt", "kiot"]):
        return "Mặt bằng kinh doanh"
    if any(k in text for k in ["kho", "nhà xưởng", "nha xuong", "xưởng", "xuong"]):
        return "Kho bãi/Nhà xưởng"

    return "Khác"


def _infer_transaction_type(ad: Dict) -> str:
    text = " ".join(
        [
            str(ad.get("type") or ""),
            str(ad.get("category_name") or ""),
            str(ad.get("subject") or ad.get("title") or ""),
        ]
    ).lower()

    if not text.strip():
        return "Khác"

    if any(k in text for k in ["cho thuê", "cho thue", "thuê", "thue"]):
        return "Cho thuê"
    if any(k in text for k in ["bán", "ban", "chuyển nhượng", "chuyen nhuong"]):
        return "Bán"

    return "Khác"


def build_detail_record(payload: Dict, ad_id: Optional[str] = None, list_id: Optional[str] = None) -> Optional[Dict]:
    ad = payload.get("ad", {})
    location = ad.get("location") or {}
    location_fields = _extract_location_fields(ad, location)

    l_id = normalize_id(ad.get("list_id")) or normalize_id(list_id) or normalize_id(ad.get("ad_id"))
    if not l_id:
        return None

    return {
        "source": "chotot",
        "external_id": normalize_id(ad.get("ad_id")) or normalize_id(ad_id) or l_id,
        "source_url": _resolve_source_url(ad, l_id),
        "title": ad.get("subject") or ad.get("title") or "",
        "description": ad.get("body") or "",
        "price_vnd": ad.get("price"),
        "area_sqm": ad.get("area"),
        "address": _build_address_text(ad, location_fields),
        "district": location_fields["district"],
        "ward": location_fields["ward"],
        "city": location_fields["city"],
        "property_type": _infer_property_type(ad),
        "transaction_type": _infer_transaction_type(ad),
        "contact_name": ad.get("account_name") or ad.get("name"),
        "contact_phone": ad.get("phone"),
        "images": _extract_images(ad),
        "raw_payload": payload,
        "crawled_at": datetime.now(UTC).isoformat(),
    }


def build_fallback_record(row: pd.Series) -> Optional[Dict]:
    l_id = normalize_id(row.get("list_id")) or normalize_id(row.get("ad_id"))
    if not l_id:
        return None

    ad = row.to_dict()
    location_fields = _extract_location_fields(ad, {})

    return {
        "source": "chotot",
        "external_id": normalize_id(row.get("ad_id")) or l_id,
        "source_url": _resolve_source_url(ad, l_id),
        "title": row.get("subject") or "",
        "description": "",
        "price_vnd": row.get("price"),
        "area_sqm": row.get("area"),
        "address": _build_address_text(ad, location_fields),
        "district": location_fields["district"],
        "ward": location_fields["ward"],
        "city": location_fields["city"],
        "property_type": _infer_property_type(ad),
        "transaction_type": _infer_transaction_type(ad),
        "contact_name": None,
        "contact_phone": None,
        "images": _extract_images(ad),
        "raw_payload": ad,
        "crawled_at": datetime.now(UTC).isoformat(),
    }


