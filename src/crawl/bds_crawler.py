import sys
from pathlib import Path
import time
import random
import re
import os
from typing import Dict, Optional
import pandas as pd
from playwright.sync_api import sync_playwright, Route, Playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth

# Cho phép chạy trực tiếp file
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BASE_URL = "https://batdongsan.com.vn/ban-nha-dat-ha-noi"

class BDSCrawler:
    playwright: Playwright
    browser: Browser
    context: BrowserContext
    page: Page

    def __init__(self, headless: bool = True, user_data_dir: Optional[str] = None) -> None:
        self.headless = headless
        self.playwright = sync_playwright().start()
        
        # Thu muc luu du lieu trinh duyet
        base_dir = "/opt/airflow" if os.path.exists("/opt/airflow") else os.getcwd()
        if not user_data_dir:
            user_data_dir = os.path.join(base_dir, "browser_profile")
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
        
        print(f"BDSCrawler initialized (headless={headless}, persistent=True)", flush=True)
        Stealth().apply_stealth_sync(self.page)

    def _handle_cloudflare(self):
        """San va click Cloudflare Turnstile neu xuat hien."""
        # Kiem tra nhanh xem co Cloudflare khong
        content = self.page.content()
        if "cloudflare" not in content.lower() and "challenge" not in content.lower() and "verify you are human" not in content.lower():
            return False

        print("[SHADOW] Phat hien Cloudflare, dang tu dong xu ly...", flush=True)
        for i in range(10): 
            try:
                for frame in self.page.frames:
                    if "cloudflare" in frame.url or "challenge" in frame.url or "turnstile" in frame.url:
                        for selector in ['#challenge-stage', 'input[type="checkbox"]', '.ctp-checkbox-label']:
                            target = frame.locator(selector)
                            if target.count() > 0:
                                print(f"[SHADOW] Click Turnstile thanh cong!", flush=True)
                                target.click()
                                time.sleep(5)
                                return True
                
                if i == 5: # Chiêu cuối nếu sau 10s vẫn kẹt
                    self.page.mouse.click(300, 310) 
                    time.sleep(3)
            except Exception: pass
            time.sleep(2)
        return False

    def get_listing_urls(self, page_num: int = 1) -> Optional[pd.DataFrame]:
        url = f"{BASE_URL}/p{page_num}" if page_num > 1 else BASE_URL
        try:
            print(f"[Listing] Dang truy cap: {url}", flush=True)
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            self._handle_cloudflare()
            
            try:
                self.page.wait_for_selector(".js__card", timeout=20000)
            except Exception:
                log_dir = "/opt/airflow/logs" if os.path.exists("/opt/airflow") else "logs"
                if not os.path.exists(log_dir): os.makedirs(log_dir, exist_ok=True)
                path = os.path.join(log_dir, f"error_list_{int(time.time())}.png")
                self.page.screenshot(path=path)
                print(f"[DEBUG] Khong thay tin rao. Anh loi: {path}", flush=True)
                raise
            
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
        # Nghỉ ngẫu nhiên để tránh bot detection
        time.sleep(random.uniform(3.0, 7.0))
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._handle_cloudflare()
            
            try:
                self.page.wait_for_selector(".js__pr-description", timeout=20000)
            except Exception: return None
            
            desc_el = self.page.query_selector(".js__pr-description")
            description = desc_el.inner_text().strip() if desc_el else ""
            contact_name_el = self.page.query_selector(".re__contact-name")
            contact_name = contact_name_el.inner_text().strip() if contact_name_el else ""
            
            contact_phone = ""
            try:
                phone_btn = self.page.query_selector(".js__btn-tracking, .re__contact-phone")
                if phone_btn:
                    phone_btn.click(force=True)
                    time.sleep(1.5)
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

def crawl_bds_to_mongodb(pages: int = 3, headless: bool = False, user_data_dir: str = None) -> int:
    from src.crawl.bds_pipeline import crawl_bds_to_mongodb as run_pipeline
    return run_pipeline(pages=pages, fetch_detail=True, headless=headless, user_data_dir=user_data_dir)

if __name__ == "__main__":
    test_headless = "--headless" in sys.argv
    profile_arg = "browser_profile_test" if "--test-profile" in sys.argv else None
    crawl_bds_to_mongodb(pages=1, headless=test_headless, user_data_dir=profile_arg)
