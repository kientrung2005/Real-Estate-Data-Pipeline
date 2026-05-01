import sys
from pathlib import Path
import os

# Cho phép chạy trực tiếp file: Luôn đưa PROJECT_ROOT lên đầu tiên
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
import random
import re
from typing import Dict, Optional
import pandas as pd
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth

# Import cấu hình (Sau khi đã setup sys.path)
from config.settings import (
    DEFAULT_PAGES_LOCAL, SLEEP_FAST_MODE, SLEEP_SAFE_MODE,
    BROWSER_PROFILE_NAME, DEFAULT_HEADLESS, LOG_DIR_LOCAL, LOG_DIR_AIRFLOW
)
from config.selectors import BDS_SELECTORS

BASE_URL = "https://batdongsan.com.vn/ban-nha-dat-ha-noi"

class BDSCrawler:
    playwright: Playwright
    browser: Browser
    context: BrowserContext
    page: Page

    def __init__(self, headless: bool = DEFAULT_HEADLESS, user_data_dir: Optional[str] = None) -> None:
        self.headless = headless
        # Tự động bật Fast Mode nếu không chạy trong Docker/Airflow
        self.is_airflow = os.path.exists("/opt/airflow")
        self.fast_mode = not self.is_airflow
        
        self.playwright = sync_playwright().start()
        
        # Thư mục lưu dữ liệu trình duyệt
        base_dir = "/opt/airflow" if self.is_airflow else os.getcwd()
        if not user_data_dir:
            user_data_dir = os.path.join(base_dir, BROWSER_PROFILE_NAME)
        elif not os.path.isabs(user_data_dir):
            user_data_dir = os.path.join(base_dir, user_data_dir)
        
        # Khởi tạo Chromium với Stealth mode
        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir,
            headless=headless,
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--disable-notifications",
            ]
        )
        
        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self.page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => false})")
        
        mode_str = "FAST MODE" if self.fast_mode else "SAFE MODE (Airflow)"
        print(f"BDSCrawler initialized ({mode_str}, headless={headless})", flush=True)
        Stealth().apply_stealth_sync(self.page)

    def _handle_cloudflare(self):
        """Săn và click Cloudflare Turnstile nếu xuất hiện."""
        content = self.page.content()
        if "cloudflare" not in content.lower() and "challenge" not in content.lower() and "verify you are human" not in content.lower():
            return False

        # Trong Fast Mode chỉ thử 3 lần để tiết kiệm thời gian
        retries = 3 if self.fast_mode else 10
        print(f"[SHADOW] Phat hien Cloudflare, dang xu ly...", flush=True)
        
        for i in range(retries): 
            try:
                for frame in self.page.frames:
                    if "cloudflare" in frame.url or "challenge" in frame.url or "turnstile" in frame.url:
                        for selector in ['#challenge-stage', 'input[type="checkbox"]', '.ctp-checkbox-label']:
                            target = frame.locator(selector)
                            if target.count() > 0:
                                target.click()
                                time.sleep(2 if self.fast_mode else 5)
                                return True
                
                if not self.fast_mode and i == 5:
                    self.page.mouse.click(300, 310) 
                    time.sleep(3)
            except Exception: pass
            time.sleep(1 if self.fast_mode else 2)
        return False

    def get_listing_urls(self, page_num: int = 1) -> Optional[pd.DataFrame]:
        url = f"{BASE_URL}/p{page_num}" if page_num > 1 else BASE_URL
        try:
            print(f"[Listing] Dang truy cap: {url}", flush=True)
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            self._handle_cloudflare()
            
            try:
                self.page.wait_for_selector(BDS_SELECTORS["CARD"], timeout=10000 if self.fast_mode else 20000)
            except Exception:
                log_dir = LOG_DIR_AIRFLOW if self.is_airflow else LOG_DIR_LOCAL
                if not os.path.exists(log_dir): os.makedirs(log_dir, exist_ok=True)
                path = os.path.join(log_dir, f"error_list_{int(time.time())}.png")
                self.page.screenshot(path=path)
                print(f"[DEBUG] Khong thay tin rao. Anh loi: {path}", flush=True)
                raise
            
            listings = self.page.query_selector_all(BDS_SELECTORS["CARD"])
            data = []
            for item in listings:
                # Selector phụ cho link và ad_id
                link_el = item.query_selector("a.js__product-link-for-product-id")
                href = link_el.get_attribute("href") if link_el else ""
                full_link = f"https://batdongsan.com.vn{href}" if href and href.startswith("/") else href
                ad_id = item.get_attribute("prid") or (href.split("-pr")[-1] if href else "")
                
                title_el = item.query_selector(BDS_SELECTORS["TITLE"])
                price_el = item.query_selector(BDS_SELECTORS["PRICE"])
                area_el = item.query_selector(BDS_SELECTORS["AREA"])
                location_el = item.query_selector(BDS_SELECTORS["LOCATION"])
                
                data.append({
                    "ad_id": ad_id,
                    "url": full_link,
                    "title": title_el.inner_text().strip() if title_el else "",
                    "price": price_el.inner_text().strip() if price_el else "",
                    "area": area_el.inner_text().strip() if area_el else "",
                    "address": location_el.inner_text().strip() if location_el else ""
                })
            
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception as e:
            print(f"Loi listing page {page_num}: {e}", flush=True)
            return None

    def get_property_detail(self, url: str) -> Optional[Dict]:
        if not url: return None
        # FAST MODE thì nghỉ ít thôi
        wait_range = SLEEP_FAST_MODE if self.fast_mode else SLEEP_SAFE_MODE
        time.sleep(random.uniform(*wait_range))
        
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._handle_cloudflare()
            
            try:
                self.page.wait_for_selector(BDS_SELECTORS["DESCRIPTION"], timeout=10000 if self.fast_mode else 20000)
            except Exception: return None
            
            desc_el = self.page.query_selector(BDS_SELECTORS["DESCRIPTION"])
            description = desc_el.inner_text().strip() if desc_el else ""
            contact_name_el = self.page.query_selector(BDS_SELECTORS["CONTACT_NAME"])
            contact_name = contact_name_el.inner_text().strip() if contact_name_el else ""
            
            contact_phone = ""
            try:
                phone_btn = self.page.query_selector(BDS_SELECTORS["PHONE_BTN"])
                if phone_btn:
                    phone_btn.click(force=True)
                    time.sleep(0.5 if self.fast_mode else 1.5)
                body_text = self.page.locator("body").inner_text()
                clean_body = body_text.replace(" ", "").replace(".", "").replace("-", "")
                matched = re.findall(r'(0[35789][0-9]{8})', clean_body)
                if matched: contact_phone = matched[0]
            except Exception: pass
            
            return {
                "source_url": url,
                "description": description,
                "contact_name": contact_name,
                "contact_phone": contact_phone
            }
        except Exception as e:
            print(f"Loi chi tiet {url}: {e}", flush=True)
            return None
            
    def close(self):
        if hasattr(self, 'context'): self.context.close()
        if hasattr(self, 'playwright'): self.playwright.stop()

def crawl_bds_to_mongodb(pages: int = DEFAULT_PAGES_LOCAL, headless: bool = DEFAULT_HEADLESS, user_data_dir: str = None) -> int:
    from src.crawl.bds_pipeline import crawl_bds_to_mongodb as run_pipeline
    return run_pipeline(pages=pages, fetch_detail=True, headless=headless, user_data_dir=user_data_dir)

if __name__ == "__main__":
    test_headless = "--headless" in sys.argv
    crawl_bds_to_mongodb(pages=DEFAULT_PAGES_LOCAL, headless=test_headless)
