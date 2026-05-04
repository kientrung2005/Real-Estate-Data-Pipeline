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
        self.is_airflow = os.path.exists("/opt/airflow")
        self.fast_mode = not self.is_airflow
        self._xvfb_proc = None
        
        # 1. Khởi tạo Xvfb nếu chạy trên Airflow (Linux)
        if self.is_airflow:
            import subprocess
            try:
                self._xvfb_proc = subprocess.Popen(
                    ["Xvfb", ":99", "-screen", "0", "1920x1080x24", "-nolisten", "tcp"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                os.environ["DISPLAY"] = ":99"
                headless = False 
                time.sleep(1)
                print("[XVFB] Man hinh ao da khoi tao.", flush=True)
            except Exception as e:
                print(f"[XVFB] Loi: {e}", flush=True)
        
        self.playwright = sync_playwright().start()
        
        # 2. Thiết lập đường dẫn Profile
        base_dir = "/opt/airflow" if self.is_airflow else os.getcwd()
        if not user_data_dir:
            user_data_dir = os.path.join(base_dir, BROWSER_PROFILE_NAME)
        
        # 3. Dọn dẹp profile cũ (Rất quan trọng để tránh lỗi TargetClosedError)
        if os.path.exists(user_data_dir):
            import shutil
            try:
                shutil.rmtree(user_data_dir, ignore_errors=True)
                time.sleep(1)
                print(f"[DEBUG] Da lam moi browser profile: {user_data_dir}", flush=True)
            except Exception: pass
        
        # 4. Cấu hình User Agent và Args theo Hệ điều hành
        if self.is_airflow:
            user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            extra_args = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        else:
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            extra_args = [] # Windows không cần các cờ sandbox của Linux

        # 5. Khởi tạo Chromium
        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir,
            headless=headless,
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True,
            user_agent=user_agent,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-notifications",
                "--disable-gpu",
            ] + extra_args
        )

        
        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self.page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => false})")
        
        mode_str = "FAST MODE" if self.fast_mode else "SAFE MODE (Airflow)"
        print(f"BDSCrawler initialized ({mode_str}, headless={headless})", flush=True)
        Stealth().apply_stealth_sync(self.page)

    def _simulate_human(self):
        """Giả lập hành vi người dùng thật."""
        try:
            for _ in range(random.randint(1, 2)):
                self.page.mouse.wheel(0, random.randint(200, 500))
                time.sleep(random.uniform(0.5, 1.0))
            self.page.mouse.move(random.randint(100, 500), random.randint(100, 500), steps=5)
        except Exception: pass

    def _handle_cloudflare(self):
        """Săn và click Cloudflare Turnstile bằng tọa độ chuẩn."""
        try:
            # Kiểm tra trạng thái đang xác thực (Verifying)
            is_verifying = "verifying" in self.page.content().lower()
            if not is_verifying:
                for f in self.page.frames:
                    if "verifying" in f.url or "verifying" in (f.content().lower() if f else ""):
                        is_verifying = True; break
            
            if is_verifying:
                print(f"[SHADOW] Dang xac thuc... dang gia lap nguoi dung.", flush=True)
                self._simulate_human()
                time.sleep(5)
                return True

            # Kiểm tra xem có Challenge không
            has_challenge = self.page.locator("iframe[src*='challenges.cloudflare.com']").count() > 0
            if not has_challenge: return False

            print(f"[SHADOW] Phat hien Cloudflare, dang click toa do...", flush=True)
            for frame in self.page.frames:
                if "challenges.cloudflare.com" in frame.url or "turnstile" in frame.url:
                    # Click tọa độ chuẩn (30, 32) bên trong iframe
                    target = frame.locator("body")
                    target.hover(position={'x': 30, 'y': 32})
                    time.sleep(0.5)
                    target.click(position={'x': 30, 'y': 32}, force=True)
                    print(f"[SHADOW] Da bam vao nut xac minh. Cho 15s...", flush=True)
                    time.sleep(15)
                    return True
        except Exception: pass
        return False

    def get_listing_urls(self, page_num: int = 1) -> Optional[pd.DataFrame]:
        url = f"{BASE_URL}/p{page_num}" if page_num > 1 else BASE_URL
        try:
            print(f"[Listing] Dang truy cap: {url}", flush=True)
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Thử giải quyết Cloudflare
            self._handle_cloudflare()
            
            # Vòng lặp chờ dữ liệu (tối đa 10 phút)
            max_wait = 600
            elapsed = 0
            while elapsed < max_wait:
                try:
                    self.page.wait_for_selector(BDS_SELECTORS["CARD"], timeout=10000)
                    break
                except Exception:
                    self._handle_cloudflare()
                    elapsed += 15
                    print(f"[SHADOW] Dang cho Cloudflare... ({elapsed}s/{max_wait}s)", flush=True)
                    if elapsed % 30 == 0:
                        log_dir = LOG_DIR_AIRFLOW if self.is_airflow else LOG_DIR_LOCAL
                        self.page.screenshot(path=os.path.join(log_dir, f"wait_{elapsed}.png"))
            
            listings = self.page.query_selector_all(BDS_SELECTORS["CARD"])
            data = []
            for item in listings:
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
        wait_range = SLEEP_FAST_MODE if self.fast_mode else SLEEP_SAFE_MODE
        time.sleep(random.uniform(*wait_range))
        
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._handle_cloudflare()
            
            try:
                self.page.wait_for_selector(BDS_SELECTORS["DESCRIPTION"], timeout=15000)
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
        if hasattr(self, '_xvfb_proc') and self._xvfb_proc:
            self._xvfb_proc.terminate()
            print("[XVFB] Da tat man hinh ao.", flush=True)

def crawl_bds_to_mongodb(pages: int = DEFAULT_PAGES_LOCAL, headless: bool = DEFAULT_HEADLESS, user_data_dir: str = None) -> int:
    from src.crawl.bds_pipeline import crawl_bds_to_mongodb as run_pipeline
    return run_pipeline(pages=pages, fetch_detail=True, headless=headless, user_data_dir=user_data_dir)

if __name__ == "__main__":
    test_headless = "--headless" in sys.argv
    crawl_bds_to_mongodb(pages=DEFAULT_PAGES_LOCAL, headless=test_headless)
