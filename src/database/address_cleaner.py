"""Logic làm sạch và chuẩn hóa địa chỉ bất động sản."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse


# Bản đồ phục hồi dấu tiếng Việt cho tên đường và tên hành chính


ADMIN_ACCENT_MAP = {
    # Quận/Huyện
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
    "hoang mai": "Hoàng Mai",
    "thanh xuan": "Thanh Xuân",
    "hai ba trung": "Hai Bà Trưng",
    "long bien": "Long Biên",
    "ba dinh": "Ba Đình",
    "hoan kiem": "Hoàn Kiếm",
    "thanh tri": "Thanh Trì",
    "son tay": "Sơn Tây",
    "quoc oai": "Quốc Oai",
    "chuong my": "Chương Mỹ",
    "thach that": "Thạch Thất",
    # Phường/Xã/Thị trấn
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
    "tram troi": "Trạm Trôi",
    "lang thuong": "Láng Thượng",
    "lang ha": "Láng Hạ",
    "duong noi": "Dương Nội",
    "van quan": "Văn Quán",
    "phu la": "Phú La",
    "yen nghia": "Yên Nghĩa",
    "dai mo": "Đại Mỗ",
    "phu do": "Phú Đô",
    "me tri": "Mễ Trì",
    "my dinh": "Mỹ Đình",
    "phuc dien": "Phúc Diễn",
    "xuan phuong": "Xuân Phương",
    "co nhue": "Cổ Nhuế",
    "duc giang": "Đức Giang",
    "duc thuong": "Đức Thượng",
    "duc thuong dong": "Đức Thượng",
    "ngoc thuy": "Ngọc Thụy",
    "phuc loi": "Phúc Lợi",
    "viet hung": "Việt Hưng",
    "sai dong": "Sài Đồng",
    "bo de": "Bồ Đề",
    "khuong dinh": "Khương Đình",
    "dai thanh": "Đại Thành",
    "thanh xuan bac": "Thanh Xuân Bắc",
    "thanh xuan nam": "Thanh Xuân Nam",
    "thanh xuan trung": "Thanh Xuân Trung",
    "o cho dua": "Ô Chợ Dừa",
    "tuong mai": "Tương Mai",
    "van chuong": "Văn Chương",
    "phuc dong": "Phúc Đồng",
    "dang xa": "Đặng Xá",
    "kim lien": "Kim Liên",
    "mai dich": "Mai Dịch",
    "thinh quang": "Thịnh Quang",
    "hang bot": "Hàng Bột",
    "cat linh": "Cát Linh",
    "kham thien": "Khâm Thiên",
    "phuong lien": "Phương Liên",
    "phuong mai": "Phương Mai",
    "van mieu": "Văn Miếu",
    "quoc tu giam": "Quốc Tử Giám",
    "co dong": "Cổ Đông",
    "nghia tan": "Nghĩa Tân",
    "ha cau": "Hà Cầu",
    "phu cat": "Phú Cát",
    "nhan chinh": "Nhân Chính",
    "dich vong": "Dịch Vọng",
    "dich vong hau": "Dịch Vọng Hậu",
    "yen hoa": "Yên Hòa",
    "trung hoa": "Trung Hòa",
    "thuong dinh": "Thượng Đình",
    "ha dinh": "Hạ Đình",
    "khuong trung": "Khương Trung",
    "khuong mai": "Khương Mai",
    "phuong liet": "Phương Liệt",
    "kim giang": "Kim Giang",
    "mo lao": "Mỗ Lao",
    "phuc la": "Phúc La",
    "kien hung": "Kiến Hưng",
    "phu lam": "Phú Lãm",
    "phu luong": "Phú Lương",
    "la khe": "La Khê",
    "van phuc": "Vạn Phúc",
    "quang trung": "Quang Trung",
    "nguyen trai": "Nguyễn Trãi",
    "yet kieu": "Yết Kiêu",
    "an khanh": "An Khánh",
    "an thuong": "An Thượng",
    "di trach": "Di Trạch",
    "van canh": "Vân Canh",
    "la phu": "La Phù",
    "bach khoa": "Bách Khoa",
    "dong tam": "Đồng Tâm",
    "bach dang": "Bạch Đằng",
    "thanh luong": "Thanh Lương",
    "thanh nhan": "Thanh Nhàn",
    "minh khai": "Minh Khai",
    "truong dinh": "Trương Định",
    "bach mai": "Bạch Mai",
    "cau den": "Cầu Dền",
    "vinh tuy": "Vĩnh Tuy",
    "phu dien": "Phú Diễn",
    "dai kim": "Đại Kim",
    "vinh hung": "Vĩnh Hưng",
    "kieu ky": "Kiêu Kỵ",
    "nam phuong tien": "Nam Phương Tiến",
    "uy no": "Uy Nỗ",
    "dinh cong": "Định Công",
    "bien giang": "Biên Giang",
    "yen so": "Yên Sở",
    "nhat tan": "Nhật Tân",
    "hoang liet": "Hoàng Liệt",
    "giap bat": "Giáp Bát",
    "hoang van thu": "Hoàng Văn Thụ",
    "linh nam": "Lĩnh Nam",
    "mai dong": "Mai Động",
    "tan mai": "Tân Mai",
    "thanh tri": "Thanh Trì",
    "thinh liet": "Thịnh Liệt",
    "tran phu": "Trần Phú",
    "buoi": "Bưởi",
    "thuy khue": "Thụy Khuê",
    "yen phu": "Yên Phụ",
    "tu lien": "Tứ Liên",
    "quang an": "Quảng An",
    "xuan la": "Xuân La",
    "dong du": "Đông Dư",
    "cu khoi": "Cự Khối",
    "bat trang": "Bát Tràng",
    "ngoc khanh": "Ngọc Khánh",
    "truc bach": "Trúc Bạch",
    "quan thanh": "Quán Thánh",
    "doi can": "Đội Cấn",
    "cong vi": "Cống Vị",
    "lieu giai": "Liễu Giai",
    "giang vo": "Giảng Võ",
    "kim ma": "Kim Mã",
    "ngoc ha": "Ngọc Hà",
    "trau quy": "Trâu Quỳ",
    "yen thuong": "Yên Thường",
    "yen vien": "Yên Viên",
    "duong xa": "Dương Xá",
    "le chi": "Lệ Chi",
    "phu thi": "Phú Thị",
    "gia thuy": "Gia Thụy",
    "phuc loi": "Phúc Lợi",
    "thach ban": "Thạch Bàn",
    "ngoc thuy": "Ngọc Thụy",
    "ngoc lam": "Ngọc Lâm",
    "giang bien": "Giang Biên",
    "xuan dinh": "Xuân Đỉnh",
    "dan": "Đàn",
    "thach hoa": "Thạch Hòa",
    "ngoc hoi": "Ngọc Hồi",
    "duong": "Dương Xá",
    "thach": "Thạch Bàn",
}



# Hàm chuẩn hóa text

def normalize_text(value: Optional[object]) -> Optional[str]:
    """Chuẩn hóa text thành dạng không dấu, lowercase — chỉ dùng cho so sánh/lookup."""
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


def clean_location_name(value: Optional[object]) -> Optional[str]:
    normalized = normalize_text(value)
    if not normalized:
        return None
    normalized = re.sub(r"^(quan|huyen|thi xa|phuong|xa|thi tran)\s+", "", normalized)
    normalized = re.sub(r"^(q|h|p|x|tx)\s+", "", normalized)
    normalized = re.sub(r"\b(tp|thanh pho|thanh pho ha noi|ha noi)\b", "", normalized).strip()
    normalized = re.sub(r"\bmoi$", "", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def title_case_from_normalized(normalized: str) -> str:
    return " ".join(part.capitalize() for part in normalized.split())


def unique_texts(values: Iterable[Optional[object]]) -> List[str]:
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


def infer_district_type(raw_district: Optional[object]) -> Optional[str]:
    text = normalize_text(raw_district)
    if not text:
        return None
    if text.startswith("huyen ") or text.startswith("h "):
        return "huyen"
    if text.startswith("thi xa") or text.startswith("tx "):
        return "thi_xa"
    if text.startswith("quan ") or text.startswith("q "):
        return "quan"
    return None


def district_alias_names(district_name: str, district_type: Optional[str]) -> List[str]:
    aliases: List[str] = [district_name, district_name.lower()]
    if district_type == "quan":
        aliases.extend([
            f"Quận {district_name}", f"Q. {district_name}",
            f"Q.{district_name}", f"Quan {district_name}",
        ])
    elif district_type == "huyen":
        aliases.extend([
            f"Huyện {district_name}", f"H. {district_name}",
            f"H.{district_name}", f"Huyen {district_name}",
        ])
    elif district_type == "thi_xa":
        aliases.extend([
            f"Thị xã {district_name}", f"TX. {district_name}",
            f"TX {district_name}", f"Thi xa {district_name}",
        ])
    return unique_texts(aliases)


def ward_alias_names(ward_name: str, canonical_name: str) -> List[str]:
    aliases: List[str] = [ward_name, canonical_name, canonical_name.lower()]
    cleaned = clean_location_name(ward_name)
    if cleaned:
        aliases.append(title_case_from_normalized(cleaned))
    return unique_texts(aliases)


# Hàm phục hồi dấu tiếng Việt

def _accentize_admin_name(raw_text: Optional[object]) -> Optional[str]:
    if raw_text is None:
        return None
    text = str(raw_text).strip()
    if not text:
        return None
    prefix = ""
    rest = text
    for p in ("Quận", "Huyện", "Thị xã", "Thị trấn", "Phường", "Xã", "Quan", "Huyen", "Thi xa", "Thi tran", "Phuong", "Xa"):
        if re.match(rf"^{p}\b", text, flags=re.IGNORECASE):
            prefix = p
            rest = re.sub(rf"^{p}\s+", "", text, flags=re.IGNORECASE).strip()
            # Chuẩn hóa prefix về dạng có dấu nếu nó là dạng không dấu
            prefix_map = {
                "quan": "Quận", "huyen": "Huyện", "thi xa": "Thị xã",
                "thi tran": "Thị trấn", "phuong": "Phường", "xa": "Xã"
            }
            prefix = prefix_map.get(prefix.lower(), prefix.capitalize())
            break
    key = normalize_text(rest)
    if not key:
        return text
    if key.endswith(" moi"):
        key = key[:-4].strip()
        if not key:
            return text
    pretty = ADMIN_ACCENT_MAP.get(key) or title_case_from_normalized(key)
    return f"{prefix} {pretty}".strip() if prefix else pretty


def _accentize_known_segment(segment: str) -> str:
    text = segment.strip()
    if not text:
        return text
    
    prefix = ""
    rest = text
    match = re.match(r"^(Quận|Huyện|Thị xã|Thị trấn|Phường|Xã|Quan|Huyen|Thi xa|Thi tran|Phuong|Xa)\b\s*", text, flags=re.IGNORECASE)
    if match:
        prefix = match.group(1).capitalize()
        rest = text[len(match.group(0)):]
    
    key = normalize_text(rest)
    if not key:
        return text
        
    # Ưu tiên check toàn bộ chuỗi trước
    if key in ADMIN_ACCENT_MAP:
        return f"{prefix} {ADMIN_ACCENT_MAP[key]}" if prefix else ADMIN_ACCENT_MAP[key]
    
    # Nếu không khớp hoàn toàn, thử bỏ bớt các từ ở cuối (để xử lý "Di Trach Hinode" -> "Di Trạch")
    # Chỉ áp dụng nếu có prefix (Xã/Phường) để tránh cắt nhầm tên đường
    if prefix:
        tokens = key.split()
        while len(tokens) > 1:
            tokens.pop()
            sub_key = " ".join(tokens)
            if sub_key in ADMIN_ACCENT_MAP:
                return f"{prefix} {ADMIN_ACCENT_MAP[sub_key]}"

    return text


def _is_legacy_moi_segment(segment: str) -> bool:
    """Phát hiện segment dạng 'Hoài Đức mới', 'Phường Phú Thượng mới'... để xóa bỏ."""
    key = normalize_text(segment) or ""
    if not key.endswith(" moi"):
        return False
    # Lấy phần trước "mới"
    base = key[:-4].strip()
    # Thử bỏ chữ "phuong", "xa", "quan", "huyen", "thi xa", "thi tran" nếu có
    base_no_prefix = re.sub(r"^(phuong|xa|quan|huyen|thi xa|thi tran)\s+", "", base).strip()
    
    return bool(
        base and (base in ADMIN_ACCENT_MAP) or
        base_no_prefix and (base_no_prefix in ADMIN_ACCENT_MAP)
    )


def extract_admin_location_from_listing_url(url: Optional[object]) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {"ward": None, "district": None}
    if not url:
        return result
    text = str(url).strip()
    if not text:
        return result
    parsed = urlparse(text)
    path = parsed.path
    if not path:
        return result
    
    # Tìm kiếm trong toàn bộ path vì Batdongsan có thể để location ở các segment phía trước
    search_text = path.replace("/", "-")
    
    def _sanitize_admin_slug_name(raw: str, max_tokens: int = 3) -> str:
        tokens = [t.strip() for t in raw.split("-") if t.strip()]
        new_tokens = []
        i = 0
        while i < len(tokens):
            if i < len(tokens) - 1 and tokens[i] == "dan" and tokens[i+1] == "phuong":
                new_tokens.append("dan-phuong")
                i += 2
            else:
                new_tokens.append(tokens[i])
                i += 1
        tokens = new_tokens
        
        stop_tokens = {
            "khu", "du", "du-an", "vinhomes", "residence", "city", "garden", "villas", "global",
            "smart", "park", "by", "gia", "kita", "wonder", "palace", "apartment", "tower",
            "re", "tot", "mem", "gap", "ban", "cho", "thue", "chinh-chu", "gia-re", "gia-tot",
            "phuong", "xa", "quan", "huyen", "thi-xa", "thi-tran",
            "hinode", "royal", "park", "nhat", "thi", "truong", "hon", "gia-mem",
        }
        kept: List[str] = []
        for token in tokens:
            if re.match(r"^pr\d+$", token, flags=re.IGNORECASE):
                continue
            if token in stop_tokens:
                if kept: 
                    break
                else:
                    continue 
            if token.isdigit() and len(kept) >= 1:
                break
            kept.append(token)
        if not kept:
            return ""
        if len(kept) > max_tokens:
            kept = kept[:max_tokens]
        return "-".join(kept)

    def _extract(pattern: str, prefix: str, max_tokens: int = 3) -> Optional[str]:
        match = re.search(pattern, search_text)
        if not match:
            return None
        raw_name = match.group(1).strip("-")
        if not raw_name:
            return None
        sanitized = _sanitize_admin_slug_name(raw_name, max_tokens=max_tokens)
        if not sanitized:
            return None
        pretty = _accentize_admin_name(sanitized.replace("-", " "))
        if not pretty or len(pretty) < 2:
            return None
        # Loại bỏ các từ khóa rác hoặc từ khóa trùng với prefix
        norm = normalize_text(pretty)
        if norm in ("re", "tot", "mem", "gap", "ban", "cho", "thue", "gia re", "gia tot", "phuong", "xa", "quan", "huyen", "nhat", "thi truong"):
            return None
            
        pretty = re.sub(r"\s+", " ", pretty).strip()
        return f"{prefix} {pretty}"
    if "batdongsan.com.vn" in text:
        result["district"] = _accentize_admin_name(
            _extract(r"(?<![a-z0-9])quan-([a-z0-9-]+)", "Quận")
            or _extract(r"(?<![a-z0-9])huyen-([a-z0-9-]+)", "Huyện")
            or _extract(r"(?<![a-z0-9])thi-xa-([a-z0-9-]+)", "Thị xã")
        )
        result["ward"] = _accentize_admin_name(
            _extract(r"(?<!dan)(?<!an)(?<![a-z0-9])phuong-([a-z0-9-]+)", "Phường")
            or _extract(r"(?<![a-z0-9])xa-([a-z0-9-]+)", "Xã")
            or _extract(r"(?<![a-z0-9])thi-tran-([a-z0-9-]+)", "Thị trấn")
        )
    return result


def _has_ward_token(text: str) -> bool:
    text_check = re.sub(r"\bxã\s+đàn\b|\bxã\s+dan\b", " ", text, flags=re.IGNORECASE)
    return bool(re.search(r"\b(Phường|Xã|Thị trấn)\b", text_check, flags=re.IGNORECASE))

def _has_district_token(text: str) -> bool:
    return bool(re.search(r"\b(Quận|Huyện|Thị xã)\b", text, flags=re.IGNORECASE))

def _contains_normalized(text: str, candidate: str) -> bool:
    text_norm = normalize_text(text) or ""
    cand_norm = normalize_text(candidate) or ""
    return cand_norm in text_norm

def clean_address_text(
    address: Optional[object],
    ward: Optional[object],
    district: Optional[object],
    city: Optional[object],
    listing_url: Optional[object] = None,
) -> Optional[str]:
    text = str(address).strip() if address is not None else ""
    url_loc = extract_admin_location_from_listing_url(listing_url)

    def _get_level(part: str) -> int:
        p_lower = normalize_text(part) or ""
        if p_lower in ("ha noi", "hanoi"):
            return 3
        # Xã Đàn là tên đường
        if p_lower in ("xa dan", "xa dan moi"):
            return 0
        if re.match(r"^(quận|huyện|thị xã|quan|huyen|thi xa)\b", part, flags=re.IGNORECASE):
            return 2
        if re.match(r"^(phường|xã|thị trấn|phuong|xa|thi tran)\b", part, flags=re.IGNORECASE):
            return 1
        return 0

    if text:
        text = re.sub(r"[\r\n]+", ", ", text)
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"^[\s\.,;:\-_|/\\•·]+", "", text).strip()

        text = re.sub(r"\bQ\.\s*", "Quận ", text)
        text = re.sub(r"\bH\.\s*", "Huyện ", text)
        text = re.sub(r"\bP\.\s*", "Phường ", text)
        text = re.sub(r"\bX\.\s*", "Xã ", text)
        text = re.sub(r"\bTX\.\s*", "Thị xã ", text)
        text = re.sub(r"\b(?:Ha\s*Noi|Hà\s*Nội)\s*mới\b", "Hà Nội", text, flags=re.IGNORECASE)
        text = re.sub(r"\bDan Phuong\b", "Đan Phượng", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()

        ward_raw = str(ward).strip() if ward else None
        ward_candidate: Optional[str] = ward_raw or url_loc.get("ward")
        district_raw = str(district).strip() if district else None
        district_candidate: Optional[str] = district_raw or url_loc.get("district")

        if ward_candidate and not _has_ward_token(text) and not _contains_normalized(text, ward_candidate):
            text = f"{text}, {ward_candidate}"
        if district_candidate and not _has_district_token(text) and not _contains_normalized(text, district_candidate):
            text = f"{text}, {district_candidate}"

        text = re.sub(
            r"(?<!,)(?<!Phường)(?<!Phuong)(?<!Xã)(?<!Xa)(?<!Quận)(?<!Quan)(?<!Huyện)(?<!Huyen)(?<!Thị)(?<!Thi)\s+\b(Phuong|Xa|Thi tran|Quan|Huyen|Thi xa|Phường|Xã|Thị trấn|Quận|Huyện|Thị xã)\b",
            r", \1",
            text, flags=re.IGNORECASE
        )

        parts = [p.strip() for p in text.split(",") if p and p.strip()]
        parts = [p for p in parts if (normalize_text(p) or "") not in ("ha noi moi", "ha noi moi hanoi")]
        parts = [p for p in parts if not _is_legacy_moi_segment(p)]
        
        non_city = [p for p in parts if (normalize_text(p) or "") not in ("ha noi", "hanoi")]
        parts = non_city + ["Hà Nội"]
        parts = [_accentize_known_segment(p) for p in parts]
        seen_norm: set = set()
        deduped = []
        for p in parts:
            key = normalize_text(p) or p
            if key not in seen_norm:
                seen_norm.add(key)
                deduped.append(p)
        
        deduped.sort(key=_get_level)
        
        # Lọc nâng cao: mỗi cấp chỉ giữ lại 1 segment duy nhất
        final_parts = []
        level_map: Dict[int, List[str]] = {1: [], 2: [], 3: []}
        for p in deduped:
            lvl = _get_level(p)
            if lvl > 0:
                name_only = clean_location_name(p)
                if not name_only or normalize_text(name_only) in ("phuong", "xa", "quan", "huyen", "thi xa", "thi tran"):
                    continue
                level_map[lvl].append(p)
        
        # Xử lý District (Level 2)
        districts = level_map[2]
        district_to_keep = districts[0] if districts else None
        
        # Xử lý Ward (Level 1)
        wards = level_map[1]
        ward_to_keep = wards[-1] if wards else None
        
        if ward_to_keep:
            final_parts.append(ward_to_keep)
        if district_to_keep:
            final_parts.append(district_to_keep)
        final_parts.append("Hà Nội")
        
        text = ", ".join(final_parts)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return text

    # Fallback khi address rỗng
    fallback_parts: List[str] = []
    ward_raw = str(ward).strip() if ward else None
    district_raw = str(district).strip() if district else None
    for part in [ward_raw, url_loc.get("ward"), district_raw, url_loc.get("district"), city]:
        if not part:
            continue
        token = re.sub(r"^[\s\.,;:\-_|/\\•·]+", "", str(part).strip()).strip()
        key = normalize_text(token) or ""
        if token and key not in ("ha noi moi",):
            fallback_parts.append(token)
    if not fallback_parts:
        return None
    non_city = [p for p in fallback_parts if (normalize_text(p) or "") not in ("ha noi", "hanoi")]
    seen_norm2: set = set()
    deduped2 = []
    for p in non_city:
        key = normalize_text(p) or p
        if key not in seen_norm2:
            seen_norm2.add(key)
            deduped2.append(p)
    return ", ".join(deduped2 + ["Hà Nội"])
