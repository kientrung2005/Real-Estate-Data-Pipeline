"""Biến đổi payload thô của Batdongsan.com.vn thành bản ghi raw đã chuẩn hóa."""

import re
from datetime import UTC, datetime
from typing import Dict, Optional
import pandas as pd
from src.crawl.utils.normalizers import normalize_id

def parse_bds_area(area_str: str) -> Optional[float]:
    """Chuyển '75 m2' hoặc '75,5 m2' thành 75.5 (float)."""
    if not area_str:
        return None
    try:
        # Lấy phần số, đổi dấu phẩy thành dấu chấm
        match = re.search(r'([\d,\.]+)', area_str)
        if match:
            val = match.group(1).replace(',', '.')
            return float(val)
    except:
        pass
    return None

def parse_bds_price(price_str: str, area_sqm: Optional[float] = None) -> Optional[float]:
    """Chuyển '1.5 tỷ', '800 triệu', '45 triệu/m2' thành con số VND."""
    if not price_str or "Thỏa thuận" in price_str:
        return None
    try:
        price_str = price_str.lower().replace(',', '.')
        # Lấy phần số
        match = re.search(r'([\d,\.]+)', price_str)
        if not match:
            return None
        
        number = float(match.group(1))
        
        if "tỷ" in price_str:
            return number * 1_000_000_000
        if "triệu/m2" in price_str or "triệu/m²" in price_str:
            if area_sqm:
                return number * 1_000_000 * area_sqm
            return None # Không tính được nếu thiếu diện tích
        if "triệu" in price_str:
            return number * 1_000_000
            
    except:
        pass
    return None

def infer_property_type(title: str) -> Optional[str]:
    """Suy luận loại hình Bất động sản từ Tiêu đề tin đăng (Batdongsan luôn bắt đầu bằng Loại hình)."""
    if not title:
        return None
        
    t = title.lower()
    
    # Map sang chuẩn Categorization chung (giống ChoTot)
    if "chung cư" in t or "căn hộ" in t or "condotel" in t or "penthouse" in t:
        return "Căn hộ/Chung cư"
    if "biệt thự" in t or "liền kề" in t or "villa" in t:
        return "Nhà biệt thự/Liền kề"
    if "mặt phố" in t or "mặt tiền" in t or "shophouse" in t:
        return "Nhà mặt phố/Shophouse"
    if "nhà riêng" in t or "nhà ngõ" in t or "nhà hẻm" in t or "bán nhà" in t:
        return "Nhà riêng/Nhà ngõ hẻm" # Fallback chung cho nhà đất thổ cư
    if "đất nền" in t or "đất thổ cư" in t or "bán đất" in t:
        return "Đất nền/Đất thổ cư"
    if "phòng trọ" in t or "nhà trọ" in t:
        return "Phòng trọ"
    if "mặt bằng" in t or "cửa hàng" in t or "ki ốt" in t or "kiot" in t:
        return "Mặt bằng kinh doanh"
    if "kho" in t or "nhà xưởng" in t:
        return "Kho bãi/Nhà xưởng"
        
    return "Khác"

def infer_transaction_type(title: str, url: str) -> str:
    """Suy luận loại hình giao dịch (Bán / Cho thuê) từ Tiêu đề hoặc URL."""
    text_to_check = (title + " " + url).lower()
    if not text_to_check.strip():
        return "Khác"
        
    if "cho-thue" in text_to_check or "cho thuê" in text_to_check or "cho thue" in text_to_check:
        return "Cho thuê"
        
    if "ban-" in text_to_check or "bán" in text_to_check or "chuyển nhượng" in text_to_check:
        return "Bán"
        
    return "Khác"

def _extract_location_fields(address_str: str) -> Dict[str, Optional[str]]:
    if not address_str:
        return {"city": None, "district": None, "ward": None, "address": None}
        
    address_str = address_str.strip(' .-,')
    
    city, district, ward = None, None, None
    
    # Pattern Q. / H. / Quận / Huyện
    dist_match = re.search(r'(?:Q\.|H\.|Quận|Huyện|TX\.|Thị xã)\s+([^,.\(]+)', address_str, re.IGNORECASE)
    if dist_match:
        district = dist_match.group(1).strip()
        
    # Pattern P. / Phường / Xã (Thêm dấu ) vào blacklist để tránh Gia Lâm mới) )
    ward_match = re.search(r'(?:P\.|Phường|X\.|Xã)\s+([^,.\(\)]+)', address_str, re.IGNORECASE)
    if ward_match:
        ward = ward_match.group(1).strip()
        
    parts = [p.strip() for p in address_str.split(',')]
    if not city:
        city = parts[-1] if len(parts) > 0 else None
    if not district and len(parts) > 1:
        district = parts[-2]
    if not ward and len(parts) > 2:
        ward = parts[-3]
    
    return {
        "city": city,
        "district": district,
        "ward": ward,
        "address": address_str,
    }

def build_bds_record(row_data: Dict, detail_data: Optional[Dict] = None) -> Optional[Dict]:
    ext_id = normalize_id(row_data.get("ad_id"))
    if not ext_id:
        return None

    address_str = row_data.get("address", "")
    location_fields = _extract_location_fields(address_str)

    price_raw = row_data.get("price", "")
    area_raw = row_data.get("area", "")
    
    area_val = parse_bds_area(area_raw)
    price_val = parse_bds_price(price_raw, area_val)

    record = {
        "source": "batdongsan",
        "external_id": ext_id,
        "source_url": row_data.get("url", ""),
        "title": row_data.get("title", ""),
        "description": "",
        "address": address_str,
        "district": location_fields["district"],
        "ward": location_fields["ward"],
        "city": location_fields["city"],
        "property_type": infer_property_type(row_data.get("title", "")),
        "transaction_type": infer_transaction_type(row_data.get("title", ""), row_data.get("url", "")),
        "contact_name": None,
        "contact_phone": None,
        "images": row_data.get("images", []),
        "raw_payload": {"list_data": row_data, "detail_data": detail_data or {}},
        "crawled_at": datetime.now(UTC).isoformat(),
    }

    if detail_data:
        record["description"] = detail_data.get("description", "")
        record["contact_name"] = detail_data.get("contact_name", "")
        record["contact_phone"] = detail_data.get("contact_phone", "")

    return record
