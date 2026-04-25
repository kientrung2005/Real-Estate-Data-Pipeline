import sys
from pathlib import Path
import time
import random
import re
from typing import Dict, Optional
import pandas as pd
from playwright.sync_api import sync_playwright, Route, Playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth

# Cho phép chạy trực tiếp file: python src/crawl/bds_crawler.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BASE_URL = "https://batdongsan.com.vn/ban-nha-dat-ha-noi"

class BDSCrawler:
    playwright: Playwright
    browser: Browser
    context: BrowserContext
    page: Page

    def __init__(self, headless: bool = True) -> None:
        self.playwright = sync_playwright().start()
        
        # Tham số tàng hình để vượt mặt các cơ chế detection
        args = [
            "--disable-blink-features=AutomationControlled",
        ]
        
        self.browser = self.playwright.chromium.launch(headless=headless, args=args)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768}
        )
        self.page = self.context.new_page()
        
        # Tiêm mã tàng hình của playwright_stealth (chuẩn v2)
        Stealth().apply_stealth_sync(self.page)
        
        # Chặn load tài nguyên thừa (ảnh, css, font) để tối ưu tốc độ
        self.page.route("**/*", self._intercept_route)

    def _intercept_route(self, route: Route) -> None:
        resource_type = route.request.resource_type
        # Tạm thời bỏ stylesheet ra khỏi blacklist vì CSS cần để hiển thị nút liên hệ
        if resource_type in ["image", "media", "font"]:
            route.abort()
        else:
            route.continue_()

    def get_listing_urls(self, page_num: int = 1) -> Optional[pd.DataFrame]:
        url = f"{BASE_URL}/p{page_num}" if page_num > 1 else BASE_URL
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            self.page.wait_for_selector(".js__card", timeout=15000)
            
            listings = self.page.query_selector_all(".js__card")
            data = []
            for item in listings:
                link_el = item.query_selector("a.js__product-link-for-product-id")
                href = link_el.get_attribute("href") if link_el else ""
                full_link = f"https://batdongsan.com.vn{href}" if href and href.startswith("/") else href
                
                ad_id = item.get_attribute("prid") or (href.split("-pr")[-1] if href else "")
                
                title_el = item.query_selector(".js__card-title")
                price_el = item.query_selector(".re__card-config-price")
                area_el = item.query_selector(".re__card-config-area")
                location_el = item.query_selector(".re__card-location")
                
                # Fetch images
                img_els = item.query_selector_all("img")
                images = []
                for img in img_els:
                    src = img.get_attribute("data-src") or img.get_attribute("src")
                    if src and src.startswith("http"):
                        images.append(src)
                
                data.append({
                    "ad_id": ad_id,
                    "url": full_link,
                    "title": title_el.inner_text().strip() if title_el else "",
                    "price": price_el.inner_text().strip() if price_el else "",
                    "area": area_el.inner_text().strip() if area_el else "",
                    "address": location_el.inner_text().strip() if location_el else "",
                    "images": list(set(images))
                })
            
            if not data:
                return pd.DataFrame()
                
            return pd.DataFrame(data)
            
        except Exception as e:
            print(f"Lỗi khi cào list page {page_num}: {e}")
            return None

    def get_property_detail(self, url: str) -> Optional[Dict]:
        """Truy cập trang chi tiết để lấy thêm thông tin."""
        if not url:
            return None
            
        time.sleep(random.uniform(1.5, 3.0))
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # CHỐT CHẶN QUAN TRỌNG: Chờ tối đa 60s để bạn TỰ CLICK MÀN HÌNH CAPTCHA
            if "Just a moment" in self.page.title() or "Cloudflare" in self.page.title():
                print("Phát hiện Cloudflare! Bạn hãy click vào ô 'Verify you are human' trên trình duyệt, script sẽ tự động đợi...")
            
            self.page.wait_for_selector(".js__pr-description", timeout=60000)
            
            desc_el = self.page.query_selector(".re__section-body.re__detail-content.js__section-body.js__pr-description") or self.page.query_selector(".js__pr-description")
            description = desc_el.inner_text().strip() if desc_el else ""
            
            attributes = {}
            specsList = self.page.query_selector_all(".re__pr-specs-content-item")
            for spec in specsList:
                title = spec.query_selector(".re__pr-specs-content-item-title")
                val = spec.query_selector(".re__pr-specs-content-item-value")
                if title and val:
                    attributes[title.inner_text().strip()] = val.inner_text().strip()
            
            contact_name_el = self.page.query_selector(".re__contact-name") or self.page.query_selector(".js__pr-contact-name")
            contact_name = contact_name_el.inner_text().strip() if contact_name_el else ""
            if not contact_name:
                alt_name = self.page.query_selector(".re__contact-title")
                contact_name = alt_name.get_attribute("title") if alt_name else ""

            # ----------------------------------------------------------------
            # Lấy địa chỉ từ trang detail — ưu tiên dòng chính, bỏ phần chú
            # thích hành chính mới trong ngoặc đơn.
            # ----------------------------------------------------------------
            address_detail = ""

            # Selector theo thứ tự ưu tiên — thử từng cái, lấy cái đầu tiên có nội dung.
            ADDRESS_SELECTORS = [
                # Dòng địa chỉ ngắn nằm dưới tiêu đề (thường đầy đủ nhất: Dự án, Xã, Huyện)
                ".re__pr-short-info-item.js__pr-address .re__pr-short-info-item-value",
                ".re__pr-short-info-item.js__pr-address",
                # Breadcrumb: Bán / Hà Nội / Huyện Đan Phượng / Nhà biệt thự...
                # → lấy phần [1] (Hà Nội) đến [-2] (tên huyện) để ghép thành chuỗi
                ".js__pr-address",
                ".re__pr-short-info-item--address",
            ]

            for sel in ADDRESS_SELECTORS:
                addr_el = self.page.query_selector(sel)
                if not addr_el:
                    continue
                raw_value = addr_el.inner_text().strip()
                if not raw_value:
                    continue
                # Flatten xuống 1 dòng, xóa ký tự rác đầu chuỗi
                cleaned = re.sub(r"[\r\n]+", ", ", raw_value)
                cleaned = re.sub(r"\s+", " ", cleaned).strip()
                cleaned = re.sub(r"^[·•\s.,;:\-_|/\\]+", "", cleaned).strip()
                # Bỏ phần chú thích địa chỉ mới trong ngoặc: (Xã Ô Diên, Hà Nội mới)
                cleaned = re.sub(r"\s*\([^)]*\)", "", cleaned).strip()
                cleaned = re.sub(r",\s*,", ",", cleaned).strip().strip(",").strip()
                if cleaned:
                    address_detail = cleaned
                    break

            # Fallback: ghép từ các span con nếu selector trên miss
            if not address_detail:
                spans = self.page.query_selector_all(".js__pr-address span, .re__pr-short-info-item.js__pr-address span")
                chunks = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                if chunks:
                    joined = ", ".join(chunks)
                    joined = re.sub(r"\s*\([^)]*\)", "", joined).strip()
                    address_detail = re.sub(r",\s*,", ",", joined).strip().strip(",")

            # ----------------------------------------------------------------
            # Lấy quận/huyện từ breadcrumb — nguồn đáng tin cậy nhất vì BDS
            # luôn render breadcrumb: Bán > Hà Nội > Huyện Đan Phượng > ...
            # ----------------------------------------------------------------
            detail_district = ""
            try:
                breadcrumb_els = self.page.query_selector_all(".re__breadcrumb a, .re__breadcrumb span")
                # Lấy phần tử thứ 3 (index 2): thường là Quận/Huyện
                if len(breadcrumb_els) >= 3:
                    raw_bc = breadcrumb_els[2].inner_text().strip()
                    # Bỏ prefix "Quận"/"Huyện" để lưu tên thuần
                    detail_district = re.sub(r"^(Quận|Huyện|Thị xã)\s+", "", raw_bc, flags=re.IGNORECASE).strip()
            except Exception:
                detail_district = ""
            
            # Cào Số Điện Thoại - Giải pháp Tối Thuận: Click và quét toàn vùng Text bằng Text Regex (bỏ qua DOM complex)
            contact_phone = ""
            try:
                # 1. Bấm nút để gọi API hiển thị số (Batdongsan vừa đổi class sang js__btn-tracking)
                phone_btn = self.page.query_selector(".js__btn-tracking, .re__btn.js__pr-phone, .js__btn-phone, .re__contact-phone, [data-microtip-label*='hiện số']")
                if phone_btn:
                    try:
                        phone_btn.click(timeout=3000, force=True)
                        self.page.wait_for_timeout(2000) # Cho API 2 giây trả số về màn hình
                    except Exception:
                        pass
                
                # 2. Rút thô toàn bộ văn bản hiển thị trên trang
                body_text = self.page.locator("body").inner_text()
                
                # Biến "090 456 789" hoặc "090.456.789" thành "090456789"
                clean_body = body_text.replace(" ", "").replace(".", "").replace("-", "")
                
                # 3. Dùng Regex săn số điện thoại di động VN chuẩn (03, 05, 07, 08, 09 kèm 8 số)
                matched_phones = re.findall(r'(0[35789][0-9]{8})', clean_body)
                
                if matched_phones:
                    # Lấy số đầu tiên tìm thấy (thường là số trên banner nổi bật hoặc box liên hệ)
                    contact_phone = matched_phones[0]
                else:
                    # Rộng mở điều kiện quét các số bị che giấu: vd '0973357***' hoặc '090xxxx'
                    masked = re.findall(r'(0[35789][0-9]{4,8}\*{2,5})', clean_body.replace("x", "*").replace("X", "*"))
                    if masked:
                        # Tự động gọt giũa và pad dải sao cho đủ 10 ký tự như ChoTot
                        digits = "".join(c for c in masked[0] if c.isdigit())
                        contact_phone = digits.ljust(10, "*")[:10]

            except Exception as e:
                print(f"Lỗi extract SDT: {e}")
            
            return {
                "source_url": url,
                "description": description,
                "attributes": attributes,
                "contact_name": contact_name,
                "contact_phone": contact_phone,
                "address": address_detail,
                "district": detail_district,
            }
        except Exception as e:
            print(f"Lỗi truy cập trang chi tiết {url}: {e}")
            return None
            
    def close(self):
        self.context.close()
        self.browser.close()
        self.playwright.stop()

def crawl_bds_to_mongodb(pages: int = 3) -> int:
    """Wrapper tương thích ngược, chuyển điều phối sang tầng pipeline."""
    from src.crawl.bds_pipeline import crawl_bds_to_mongodb as run_pipeline
    return run_pipeline(pages=pages, fetch_detail=True)

if __name__ == "__main__":
    crawl_bds_to_mongodb(pages=3)