"""Logic làm sạch và chuẩn hóa địa chỉ bất động sản."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple
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
    "dong my": "Đông Mỹ",
    "pho hue": "Phố Huế",
    "la khe": "La Khê",
    "kim ma": "Kim Mã",
    "yen so": "Yên Sở",
    "nhat tan": "Nhật Tân",
    "thuong dinh": "Thượng Đình",
    "dang xa": "Đặng Xá",
    "kim lien": "Kim Liên",
    "mai dich": "Mai Dịch",
    "dong nhan": "Đồng Nhân",
    "tu liem": "Từ Liêm",
    "son dong": "Sơn Đồng",
    "nam hong": "Nam Hồng",
    "yen nghia": "Yên Nghĩa",
    "phu la": "Phú La",
    "phu lam": "Phú Lãm",
    "dai mo": "Đại Mỗ",
    "bach khoa": "Bách Khoa",
    "quang an": "Quảng An",
    "lang ha": "Láng Hạ",
    "vinh tuy": "Vĩnh Tuy",
    "kham thien": "Khâm Thiên",
    "hang bot": "Hàng Bột",
    "cau den": "Cầu Dền",
    "dong nhan": "Đồng Nhân",
    "tu liem": "Từ Liêm",
    "son dong": "Sơn Đồng",
    "nam hong": "Nam Hồng",
    "tan hoi": "Tân Hội",
    "o dien": "Ô Diên",
    "hoa thach": "Hoa Thạch",
    "phung chau": "Phụng Châu",
    "tan trieu": "Tân Triều",
    "tu hiep": "Tứ Hiệp",
    "xuan mai": "Xuân Mai",
    "dong ngac": "Đông Ngạc",
    "cau dien": "Cầu Diễn",
    "bien giang": "Biên Giang",
    "phu do": "Phú Đô",
    "bac son": "Bắc Sơn",
    "vong la": "Vọng La",
    "hai boi": "Hải Bối",
    "kim chung": "Kim Chung",
    "le dai hanh": "Lê Đại Hành",
    "pham dinh ho": "Phạm Đình Hổ",
    "thanh van": "Thanh Văn",
    "kim bai": "Kim Bài",
    "dong mac": "Đông Mác",
    "co nhue 2": "Cổ Nhuế 2",
    "phuong canh": "Phương Canh",
    "mai lam": "Mai Lâm",
    "huu hoa": "Hữu Hòa",
    "phu dong": "Phù Đổng",
    "vinh quynh": "Vĩnh Quỳnh",
    "phu linh": "Phù Linh",
    "tho": "Phúc Thọ",
    "lang": "Láng",
    "ta thanh oai": "Tả Thanh Oai",
    "xuan non": "Xuân Nộn",
    "my dinh 1": "Mỹ Đình 1",
    "dong mai": "Đồng Mai",
    "khuong thuong": "Khương Thượng",
    "quynh mai": "Quỳnh Mai",
    "nhi khe": "Nhị Khê",
    "thuong thanh": "Thượng Thanh",
    "mai dinh": "Mai Đình",
    "van con": "Vân Côn",
    "binh yen": "Bình Yên",
    "dai mach": "Đại Mạch",
    "vo nghia": "Võ Nghĩa",
    "thanh xuan": "Thanh Xuân",
    "phu minh": "Phú Minh",
    "phu cuong": "Phú Cường",
    "ma xa": "Mai Đình",
    "quang minh": "Quang Minh",
    "chi dong": "Chi Đông",
    "thinh quang": "Thịnh Quang",
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
    "dai ang": "Đại Áng",
    "dong my": "Đông Mỹ",
    "duyen ha": "Duyên Hà",
    "huu hoa": "Hữu Hòa",
    "lien ninh": "Liên Ninh",
    "ngu hiep": "Ngũ Hiệp",
    "ta thanh oai": "Tả Thanh Oai",
    "tam hiep": "Tam Hiệp",
    "tan trieu": "Tân Triều",
    "thanh liet": "Thanh Liệt",
    "tu hiep": "Tứ Hiệp",
    "van phuc": "Vạn Phúc",
    "vinh quynh": "Vĩnh Quỳnh",
    "yen my": "Yên Mỹ",
    "van dien": "Văn Điển",
    "buoi": "Bưởi",
    "thuy khue": "Thụy Khuê",
    "yen phu": "Yên Phụ",
    "tu lien": "Tứ Liên",
    "quang an": "Quảng An",
    "hoa thach": "Hòa Thạch",
    "phuong canh": "Phương Canh",
    "xuan phuong": "Xuân Phương",
    "xuan la": "Xuân La",
    "dong du": "Đông Dư",
    "cu khoi": "Cự Khối",
    "bat trang": "Bát Tràng",
    "giang vo": "Giảng Võ",
    "kim ma": "Kim Mã",
    "ngoc ha": "Ngọc Hà",
    "thanh cong": "Thành Công",
    "dien bien": "Điện Biên",
    "vinh phuc": "Vĩnh Phúc",
    "phuc xa": "Phúc Xá",
    "nguyen trung truc": "Nguyễn Trung Trực",
    "ngoc khanh": "Ngọc Khánh",
    "truc bach": "Trúc Bạch",
    "quan thanh": "Quán Thánh",
    "doi can": "Đội Cấn",
    "cong vi": "Cống Vị",
    "lieu giai": "Liễu Giai",
    "nghia do": "Nghĩa Đô",
    "nghia tan": "Nghĩa Tân",
    "quan hoa": "Quan Hoa",
    "dich vong": "Dịch Vọng",
    "dich vong hau": "Dịch Vọng Hậu",
    "mai dich": "Mai Dịch",
    "trung hoa": "Trung Hòa",
    "yen hoa": "Yên Hòa",
    "cau dien": "Cầu Diễn",
    "my dinh 1": "Mỹ Đình 1",
    "my dinh 2": "Mỹ Đình 2",
    "phu do": "Phú Đô",
    "me tri": "Mễ Trì",
    "trung van": "Trung Văn",
    "dai mo": "Đại Mỗ",
    "tay mo": "Tây Mỗ",
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
    "thach hoa": "Thạch Hòa",
    "ngoc hoi": "Ngọc Hồi",
    "duong": "Dương Xá",
    "thach": "Thạch Bàn",
    "lang": "Láng",
    "tay tuu": "Tây Tựu",
    "dong ngac": "Đông Ngạc",
    "duc thang": "Đức Thắng",
    "lien mac": "Liên Mạc",
    "thuong cat": "Thượng Cát",
    "thuy phuong": "Thụy Phương",
    "xuan tao": "Xuân Tảo",
    "dong xuan": "Đồng Xuân",
    "hang bac": "Hàng Bạc",
    "hang dao": "Hàng Đào",
    "hang ma": "Hàng Mã",
    "hang trong": "Hàng Trống",
    "trang tien": "Tràng Tiền",
    "cua nam": "Cửa Nam",
    "cua dong": "Cửa Đông",
    "ly thai to": "Lý Thái Tổ",
    "phan chu trinh": "Phan Chu Trinh",
    "tran hung dao": "Trần Hưng Đạo",
    "giap nhi": "Giáp Nhị",
    "den lu": "Đền Lừ",
    "phu thuong": "Phú Thượng",
    "tuong mai": "Tương Mai",
    "me linh": "Mê Linh",
    "soc son": "Sóc Sơn",
    "thuong tin": "Thường Tín",
    "thanh oai": "Thanh Oai",
    "phu xuyen": "Phú Xuyên",
    "ung hoa": "Ứng Hòa",
    "my duc": "Mỹ Đức",
    "ba vi": "Ba Vì",
    "phuc tho": "Phúc Thọ",
}

# Map để kiểm tra phường thuộc quận nào (dùng để validate tránh râu ông nọ cắm cằm bà kia)
WARD_DISTRICT_MAP = {
    "Ba Đình": {
        "thanh cong", "dien bien", "vinh phuc", "phuc xa", "nguyen trung truc",
        "giang vo", "kim ma", "lieu giai", "ngoc ha", "ngoc khanh", "quan thanh",
        "truc bach", "doi can", "cong vi"
    },
    "Tây Hồ": {
        "quang an", "tu lien", "buoi", "thuy khue", "xuan la", "yen phu", "nhat tan", "phu thuong"
    },
    "Hoàng Mai": {
        "giap bat", "tan mai", "tuong mai", "hoang liet", "thinh liet", "dai kim",
        "dinh cong", "vinh hung", "linh nam", "tran phu", "yen so", "mai dong",
        "thanh tri", "hoang van thu"
    },
    "Nam Từ Liêm": {
        "my dinh 1", "my dinh 2", "me tri", "phu do", "tay mo", "dai mo", "trung van", "xuan phuong", "phuong canh", "cau dien"
    },
    "Bắc Từ Liêm": {
        "co nhue 1", "co nhue 2", "dong ngac", "duc thang", "lien mac", "minh khai", "phu dien", "phuc dien", "tay tuu", "thuong cat", "thuy phuong", "xuan dinh", "xuan tao"
    },
    "Cầu Giấy": {
        "dich vong", "dich vong hau", "mai dich", "nghia do", "nghia tan", "quan hoa", "trung hoa", "yen hoa"
    },
    "Đống Đa": {
        "cat linh", "hang bot", "kham thien", "kim lien", "lang ha", "lang thuong", "nam dong", "ngoc khanh", "o cho dua", "phuong lien", "phuong mai", "quang trung", "quoc tu giam", "thanh quang", "tho quan", "trung liet", "trung phung", "trung tu", "van chuong", "van mieu"
    },
    "Gia Lâm": {
        "trau quy", "duong xa", "da ton", "kiieu kỵ", "bat trang", "yen vien"
    },
    "Đông Anh": {
        "uy no", "xuan non", "kim chung", "vinh ngoc", "hai boi", "dong hoi", "co loa", "dai mach", "duc tu"
    },
    "Đan Phượng": {
        "tan hoi", "tan lap", "phung"
    },
    "Hoài Đức": {
        "di trach", "tram troi", "an khanh", "an thuong", "la phu"
    }
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
    
    # Đặc biệt cho Xã Đàn, Phố Sơn Tây: Không được bóc tách tiền tố
    if any(k in normalized for k in ("xa dan", "pho son tay", "duong son tay")):
        return title_case_from_normalized(normalized)
    
    # Nếu bản thân normalized đã là một slug chuẩn trong map, giữ nguyên nó
    # (Để tránh cắt nhầm "Phương" trong "Phương Canh", "Xuân" trong "Xuân Phương")
    if normalized in ADMIN_ACCENT_MAP:
        return title_case_from_normalized(normalized)
        
    normalized = re.sub(r"^(quan|huyen|thi xa|phuong|xa|thi tran|duong|ngo|hem|ngach)\s+", "", normalized)
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
    """Suy luận loại hình quận/huyện từ text hoặc tên lõi."""
    text = normalize_text(raw_district)
    if not text:
        return None
    if any(k in text for k in ("huyen ", " h ", " h.")):
        return "huyen"
    if any(k in text for k in ("thi xa", " tx ", " tx.")):
        return "thi_xa"
    if any(k in text for k in ("quan ", " q ", " q.")):
        return "quan"
        
    # Bổ sung danh sách các huyện thực tế ở Hà Nội để suy luận từ tên lõi
    districts_huyen = {
        "dong anh", "gia lam", "thanh tri", "tu liem", "me linh", 
        "soc son", "thanh oai", "chuong my", "thach that", "quoc oai",
        "dan phuong", "hoai duc", "phuc tho", "ba vi", "my duc",
        "ung hoa", "phu xuyen", "thuong tin"
    }
    core = clean_location_name(text)
    if core in districts_huyen:
        return "huyen"
    if core == "son tay":
        return "thi_xa"
        
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


def _accentize_known_segment(segment: str, district_type: str = "quan") -> str:
    text = segment.strip()
    if not text:
        return text
    
    # Lấy tên lõi (bỏ tiền tố Quận/Phường)
    core_name = clean_location_name(text)
    core_norm = normalize_text(core_name) or ""

    # Danh mục dùng chung (Thị trấn và Xã phổ biến)
    towns = {"yen vien", "trau quy", "dong anh", "phung", "tram troi", "chuc son", "xuan mai", "lien quan", "quoc oai", "thach that", "soc son", "van dinh", "dai nghia", "thuong tin", "phu xuyen", "phu minh", "tay dang", "phuc tho"}
    communes = {
        "phung chau", "van con", "binh yen", "kiieu ky", "tan hoi", "duong xa", "di trach", "uy no", "xuan non", "da ton", "o dien", "o dien moi", "tan lap", "dong du", "bat trang", "phu dong", "co dong", "phu cat", "tan trieu", "tu hiep",
        "thanh thuy", "huu hoa", "vinh quynh", "ngu hiep", "ngoc hoi", "lien ninh", "duyen ha", "dai ang", "dong my", "ta thanh oai", "tam hiep", "vinh quynh", "huu hoa", "thanh liet",
        "dong hoi", "co loa", "duc tu", "vinh ngoc", "hai boi", "nguyen khe", "tien duong"
    }
    
    # Danh sách các quận/huyện
    districts_list = {
        "tay ho", "nam tu liem", "bac tu liem", "ha dong", "dong anh", 
        "dan phuong", "hoai duc", "gia lam", "cau giay", "dong da", 
        "hoang mai", "thanh xuan", "hai ba trung", "long bien", 
        "ba dinh", "hoan kiem", "thanh tri", "son tay", "quoc oai", 
        "chuong my", "thach that", "me linh", "soc son", "thuong tin",
        "thanh oai", "phu xuyen", "ung hoa", "my duc", "ba vi", "phuc tho"
    }

    # Tìm kiếm tên khớp nhất từ ADMIN_ACCENT_MAP
    matched_slug = None
    if core_norm in ADMIN_ACCENT_MAP:
        matched_slug = core_norm
    else:
        sorted_slugs = sorted(ADMIN_ACCENT_MAP.keys(), key=len, reverse=True)
        for s in sorted_slugs:
            # Chỉ cho phép khớp phần đầu với các slug đủ dài (>5 ký tự) 
            # để tránh nhầm lẫn (ví dụ: tránh nhầm "Đường..." thành "Dương Xá")
            if len(s) > 5 and core_norm.startswith(s + " "):
                matched_slug = s
                break

    if matched_slug:
        pretty_name = ADMIN_ACCENT_MAP[matched_slug]
        core_norm = matched_slug
        
        # Tự động sửa tiền tố
        prefix = "Xã" if district_type == "huyen" else "Phường"
        
        if core_norm in districts_list:
            # Ưu tiên Huyện cho các huyện ngoại thành
            huyen_list = {"chuong my", "hoai duc", "thach that", "quoc oai", "thanh tri", "gia lam", "dong anh", "me linh", "soc son", "ba vi", "phuc tho", "dan phuong", "thanh oai", "phu xuyen", "ung hoa", "my duc", "thuong tin"}
            
            # ĐẶC BIỆT: Nếu đầu vào đã ghi rõ là Phường/Xã/Thị trấn thì KHÔNG được ép thành Quận/Huyện
            if re.search(r"\b(phường|phuong|xã|xa|thị trấn|thi tran)\b", text, flags=re.IGNORECASE):
                # Không return sớm, để trôi xuống các check Towns/Communes bên dưới
                pass
            else:
                if core_norm in huyen_list or district_type == "huyen":
                    p_fix = "Huyện"
                else:
                    p_fix = "Quận"
                if core_norm == "son tay":
                    p_fix = "Thị xã"
                return f"{p_fix} {pretty_name}"

        # Ưu tiên các danh mục đã biết (Communes/Towns) để "đè" lên các prefix sai từ đầu vào
        if core_norm in towns:
            prefix = "Thị trấn"
        elif core_norm in communes:
            prefix = "Xã"
        else:
            # Nếu KHÔNG nằm trong danh mục đặc biệt, tôn trọng tiền tố từ đầu vào
            if re.search(r"\b(thị trấn|thi tran)\b", text, flags=re.IGNORECASE):
                prefix = "Thị trấn"
            elif re.search(r"\b(phường|phuong)\b", text, flags=re.IGNORECASE):
                prefix = "Phường"
            elif re.search(r"\b(xã|xa)\b", text, flags=re.IGNORECASE):
                prefix = "Xã"
            
        return f"{prefix} {pretty_name}"
    
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
            "hinode", "royal", "park", "gia-mem",
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

    def _extract_earliest(patterns: List[Tuple[str, str]], max_tokens: int = 3) -> Optional[str]:
        matches = []
        for pattern, prefix in patterns:
            for match in re.finditer(pattern, search_text):
                matches.append((match.start(), prefix, match.group(1).strip("-")))
        if not matches:
            return None
        matches.sort(key=lambda x: x[0])
        
        for _, prefix, raw_name in matches:
            sanitized = _sanitize_admin_slug_name(raw_name, max_tokens=max_tokens)
            if not sanitized:
                continue
            pretty = _accentize_admin_name(sanitized.replace("-", " "))
            if not pretty or len(pretty) < 2:
                continue
            norm = normalize_text(pretty)
            if norm in ("re", "tot", "mem", "gap", "ban", "cho", "thue", "gia re", "gia tot", "phuong", "xa", "quan", "huyen", "nhat", "thi truong"):
                continue
            pretty = re.sub(r"\s+", " ", pretty).strip()
            return f"{prefix} {pretty}"
        return None

    if "batdongsan.com.vn" in text:
        # Sửa regex để tránh bắt nhầm 'quan' trong 'lac-long-quan'
        district_patterns = [
            (r"(?<!lac-long-)(?<!minh-)(?<![a-z0-9])quan-([a-z0-9-]+)", "Quận"),
            (r"(?<![a-z0-9])huyen-([a-z0-9-]+)", "Huyện"),
            (r"(?<![a-z0-9])thi-xa-([a-z0-9-]+)", "Thị xã")
        ]
        ward_patterns = [
            (r"\bphuong-([a-z0-9-]+)", "Phường"),
            (r"\bxa-([a-z0-9-]+)", "Xã"),
            (r"\bthi-tran-([a-z0-9-]+)", "Thị trấn")
        ]
        
        # Chỉ lấy kết quả trực tiếp từ _extract_earliest (vì hàm này đã tự thêm dấu rồi)
        result["district"] = _extract_earliest(district_patterns, max_tokens=2)
        result["ward"] = _extract_earliest(ward_patterns, max_tokens=3)

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
    title: Optional[object] = None,
    description: Optional[object] = None,
) -> Optional[str]:
    text = str(address).strip() if address is not None else ""
    url_loc = extract_admin_location_from_listing_url(listing_url)

    def _get_level(part: str) -> int:
        p_lower = normalize_text(part) or ""
        if p_lower in ("ha noi", "hanoi"):
            return 3
        # Xã Đàn, Phố Sơn Tây là tên đường
        if any(k in p_lower for k in ("xa dan", "son tay moi", "pho son tay", "duong son tay")):
            return 0
        # Danh sách các quận/huyện để đối chiếu
        districts = {
            "tay ho", "nam tu liem", "bac tu liem", "ha dong", "dong anh", 
            "dan phuong", "hoai duc", "gia lam", "cau giay", "dong da", 
            "hoang mai", "thanh xuan", "hai ba trung", "long bien", 
            "ba dinh", "hoan kiem", "thanh tri", "son tay", "quoc oai", 
            "chuong my", "thach that", "me linh", "soc son", "thuong tin",
            "thanh oai", "phu xuyen", "ung hoa", "my duc", "ba vi", "phuc tho"
        }
        
        core_name = clean_location_name(part)
        core_norm = normalize_text(core_name) or ""
        
        # Nếu không có tên lõi (chỉ có tiền tố rỗng), đây không phải là cấp hành chính hợp lệ
        if not core_norm:
            return 0
            
        # 1. Nhận diện theo tiền tố ĐÃ chuẩn hóa (từ _accentize_known_segment)
        if part.startswith("Phường") or part.startswith("Xã") or part.startswith("Thị trấn"):
            return 1
        if part.startswith("Quận") or part.startswith("Huyện") or part.startswith("Thị xã"):
            return 2
            
        # 2. Nhận diện các từ khóa Quận/Huyện chuẩn (cho các phần chưa chuẩn hóa)
        if re.match(r"^(quận|huyện|thị xã|huyen|thi xa)\b", part, flags=re.IGNORECASE):
            return 2
            
        # 3. Bổ sung: Nếu tên nằm trong ADMIN_ACCENT_MAP 
        if p_lower in ADMIN_ACCENT_MAP or core_norm in ADMIN_ACCENT_MAP:
            return 2 if core_norm in districts else 1
                
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
        # Xóa các nội dung trong ngoặc đơn nếu có chứa từ "mới"
        text = re.sub(r"\([^)]*mới[^)]*\)", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bDan Phuong\b", "Đan Phượng", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        
        # Nếu thiếu phường, thử tìm trong title/description hoặc chính address
        if not _has_ward_token(text):
            # Chuẩn hóa tên quận để tra cứu map (ví dụ: "Quận Ba Đình" -> "Ba Đình")
            d_name_raw = str(district) if district else ""
            d_core = clean_location_name(d_name_raw)
            # Khôi phục dấu từ ADMIN_ACCENT_MAP cho d_core
            d_core_norm = normalize_text(d_core) or ""
            d_name_clean = ADMIN_ACCENT_MAP.get(d_core_norm, d_core)
            
            known_wards = WARD_DISTRICT_MAP.get(d_name_clean, set())
            search_pool = f"{text} {str(title or '')} {str(description or '')}"
            
            for kw in sorted(known_wards, key=len, reverse=True):
                if _contains_normalized(search_pool, kw):
                    pretty_kw = ADMIN_ACCENT_MAP.get(kw, title_case_from_normalized(kw))
                    # Thêm vào chuỗi để các bước sau xử lý
                    text = f"{text}, Phường {pretty_kw}"
                    break

        ward_raw = str(ward).strip() if ward else None
        # Ưu tiên lấy từ URL trước vì URL thường chứa slug chuẩn của hệ thống (ví dụ: tan-hoi)
        ward_candidate: Optional[str] = url_loc.get("ward") or ward_raw
        district_raw = str(district).strip() if district else None
        district_candidate: Optional[str] = url_loc.get("district") or district_raw

        if ward_candidate and not _has_ward_token(text) and not _contains_normalized(text, ward_candidate):
            text = f"{text}, {ward_candidate}"
        if district_candidate and not _has_district_token(text) and not _contains_normalized(text, district_candidate):
            text = f"{text}, {district_candidate}"

        text = re.sub(
            r"(?<!,)(?<!Phường)(?<!Phuong)(?<!Xã)(?<!Xa)(?<!Quận)(?<!Quan)(?<!Huyện)(?<!Huyen)(?<!Thị)(?<!Thi)\s+\b(Phuong|Xa|Thi tran|Quan|Huyen|Thi xa|Phường|Xã|Thị trấn|Quận|Huyện|Thị xã)\b",
            r", \1",
            text, flags=re.IGNORECASE
        )

        # Tách theo dấu phẩy và cả dấu ngoặc đơn
        text = text.replace("(", ",").replace(")", ",")
        parts = [p.strip() for p in text.split(",") if p and p.strip()]
        
        # 1. Chuẩn hóa tất cả các phần (bao gồm cả việc bỏ chữ "mới" ở đuôi)
        d_type = infer_district_type(str(district)) if district else "quan"
        parts = [_accentize_known_segment(p, d_type) for p in parts]
        
        # 2. Loại bỏ các phần trùng lặp hoặc vô nghĩa (như Hà Nội, Hà Nội)
        seen_norm: set = set()
        deduped = []
        for p in parts:
            if not p or normalize_text(p) in ("xa", "phuong", "thi tran", "quan", "huyen"):
                continue
            # Chống trùng lặp dựa trên cả tên lõi và Tiền tố (để giữ cả Phường Long Biên và Quận Long Biên)
            key = normalize_text(p) or p
            if key not in seen_norm and key not in ("ha noi moi", "ha noi hanoi") and "moi" not in key:
                seen_norm.add(key)
                deduped.append(p)
        
        # 4. Gom nhóm theo cấp độ
        level_map: Dict[int, List[str]] = {1: [], 2: [], 3: []}
        for p in deduped:
            lvl = _get_level(p)
            if lvl > 0:
                level_map[lvl].append(p)
        
        # 5. Xây dựng chuỗi kết quả cuối cùng theo thứ tự: Phường -> Quận -> Thành phố
        final_parts = []
        
        # --- XỬ LÝ PHƯỜNG (LEVEL 1) ---
        wards = level_map[1]
        if wards:
            final_parts.append(wards[0])
        else:
            # Dự phòng 1: Nếu website có tham số ward, sử dụng nó
            if ward:
                pw = _accentize_known_segment(str(ward), d_type)
                if pw:
                    final_parts.append(pw)
            
            # Dự phòng 2: Nếu vẫn chưa có phường, tìm trong Level 0
            if not final_parts:
                for p in deduped:
                    if _get_level(p) == 0:
                        p_norm = normalize_text(p) or ""
                        if not re.match(r"^(ql|tl|quoc lo|tinh lo|duong|pho|ngo|ngach|so|kiet|hem)\b", p_norm):
                            final_parts.append(p)
                            break
            
        # --- XỬ LÝ QUẬN (LEVEL 2) ---
        districts_found = level_map[2]
        if districts_found:
            final_parts.append(districts_found[0])
        elif district:
            pd = _accentize_known_segment(str(district), d_type)
            if pd:
                final_parts.append(pd)
            
        # --- XỬ LÝ THÀNH PHỐ (LEVEL 3) ---
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