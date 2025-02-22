import asyncio
import aiohttp
import random
import sqlite3
import json
import os
import logging
import re
import socket
from urllib.parse import urlparse, urlunparse
from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from waybackpy import WaybackMachineCDXServerAPI, exceptions
from bs4 import BeautifulSoup
import sys
from colorama import just_fix_windows_console
from colorama import Fore, Back, Style
from colorama import init
from ssl import SSLCertVerificationError

init(autoreset=True)
just_fix_windows_console()

# Windows için özel event loop ayarı
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Yapılandırma
DB_FILE = "crawled_data.db"
MAX_CONCURRENT_REQUESTS = 15
REQUEST_TIMEOUT = 120
RETRY_ATTEMPTS = 3
WAIT_TIMES = [1, 3, 5]
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

class RetryableError(Exception):
    pass

class Crawler:
    def __init__(self):
        self.ua = UserAgent()
        self.user_agents = [
            self.ua.chrome,
            self.ua.firefox,
            self.ua.safari,
            self.ua.random
        ]
        
        # Özel header'lar
        self.special_headers = {
            "www.wordfence.com": {
                "Referer": "https://www.wordfence.com/",
                "X-Forwarded-For": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
            }
        }

    async def initialize(self):
        """Playwright ve veritabanını başlat"""
        self.conn = sqlite3.connect(DB_FILE)
        self._init_db()
        
        # Playwright'ı başlat
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=True,  # Headless modu kapat
            args=["--disable-blink-features=AutomationControlled"]
        )
        logging.info("Playwright and browser initialized successfully.")

    def _init_db(self):
        """Veritabanını başlat"""
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS crawled_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cve_id TEXT,
                    url TEXT UNIQUE,
                    html TEXT,
                    source TEXT,
                    status_code INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )""")

    async def _sanitize_url(self, url):
        """URL normalizasyonu"""
        # Çift scheme temizleme
        url = re.sub(r"^(https?://)(https?://)", r"\1", url)
        parsed = urlparse(url)
        if not parsed.scheme:
            url = f"http://{url}"
            parsed = urlparse(url)
        return urlunparse((
            parsed.scheme,
            parsed.netloc.lower(),
            parsed.path.rstrip('/'),
            parsed.params,
            parsed.query,
            parsed.fragment
        ))

    async def _handle_202(self, page):
        """HTTP 202 durumunda bekleyip yönlendirme sonrası sayfayı kaydetme"""
        logging.info(f"{Fore.CYAN}[HTTP 202] {Style.RESET_ALL}Handling challenge...: {page.url}")
        await page.wait_for_timeout(5000)
        await page.reload(wait_until="networkidle")
        logging.info(f"{Fore.YELLOW}[HTTP 202] {Style.RESET_ALL}Redirected URL: {page.url}")
        return await page.content()

    async def fetch_aiohttp(self, session, url, ssl=True):
        headers = {
            "User-Agent": random.choice(self.user_agents),
            **self.special_headers.get(urlparse(url).netloc, {})
        }
        try:
            async with session.get(url, headers=headers, ssl=ssl,
                                   timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                if resp.status == 202:
                    logging.info(f"{Fore.BLUE}[HTTP 202 - Needs browser handling] {Style.RESET_ALL}: {url}")
                    # 202 durumunda, playwright ile yeniden dene
                    return await self.fetch_playwright(url)
                    
                # İçerik türünü kontrol et
                content_type = resp.headers.get("Content-Type", "")
                if "application/pdf" in content_type or "image" in content_type:
                    content = await resp.read()
                else:
                    content = await resp.text(encoding="utf-8", errors="replace")
                return {
                    "content": content,
                    "status": resp.status,
                    "source": "aiohttp"
                }
        except SSLCertVerificationError:
            logging.warning(f"{Fore.CYAN}[Try: SSL -> FALSE] {Style.RESET_ALL}: {url}")
            return await self.fetch_aiohttp(session, url, ssl=False)
        except Exception as e:
            logging.error(f"{Fore.RED}[aiohttp error] {Style.RESET_ALL}: {str(e)}")
            return None

    async def fetch_playwright(self, url):
        """Playwright ile içerik çekme"""
        context = await self.browser.new_context(
            user_agent=random.choice(self.user_agents),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            bypass_csp=True,
            ignore_https_errors=True
        )
        try:
            page = await context.new_page()
            # Tracker engelleme
            await page.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in {"image", "stylesheet", "font"} 
                else route.continue_()
            )
            response = await page.goto(url, wait_until="networkidle", timeout=90000)
            if response.status == 202:
                content = await self._handle_202(page)
            else:
                content = await page.content()
            return {
                "content": content,
                "status": response.status,
                "source": "playwright"
            }
        except Exception as e:
            logging.error(f"{Fore.RED}[Playwright error] {Style.RESET_ALL}: {str(e)}")
            return None
        finally:
            await context.close()

    async def fetch_wayback(self, url):
        """Wayback Machine'den içerik alma"""
        try:
            cdx = WaybackMachineCDXServerAPI(
                url,
                user_agent=random.choice(self.user_agents),
                start_timestamp="20200101",
                end_timestamp="20241231",
                collapses=["timestamp:6"]
            )
            snapshots = list(cdx.snapshots())
            if not snapshots:
                return None
            for snapshot in reversed(snapshots):
                async with aiohttp.ClientSession() as session:
                    async with session.get(snapshot.archive_url, timeout=REQUEST_TIMEOUT) as resp:
                        if resp.status == 200:
                            content = await resp.text()
                            if "This page is not available" not in content:
                                return {
                                    "content": content,
                                    "status": 200,
                                    "source": "wayback"
                                }
            return None
        except exceptions.NoCDXRecordFound:
            return None
        except Exception as e:
            logging.error(f"{Fore.RED}[Wayback error] {Style.RESET_ALL}: {str(e)}")
            return None

    async def process_url(self, session, cve_id, url):
        """URL işleme ana mantığı"""
        try:
            clean_url = await self._sanitize_url(url)
            result = None
            sources = [
                ("aiohttp", lambda: self.fetch_aiohttp(session, clean_url)),
                ("playwright", lambda: self.fetch_playwright(clean_url)),
                ("wayback", lambda: self.fetch_wayback(clean_url))
            ]
            for source_name, fetcher in sources:
                for attempt in range(RETRY_ATTEMPTS):
                    try:
                        async with SEMAPHORE:
                            result = await fetcher()
                            if result and result["content"]:
                                break
                            await asyncio.sleep(WAIT_TIMES[attempt])
                    except RetryableError as e:
                        logging.warning(f"{Fore.YELLOW}[Retrying] {Style.RESET_ALL}{source_name}: {str(e)}")
                        continue
                if result:
                    break
            if result:
                await self._save_result(cve_id, clean_url, result)
            else:
                logging.error(f"All sources failed: {clean_url}")
        except Exception as e:
            logging.error(f"Critical error: {str(e)}")

    async def _save_result(self, cve_id, url, result):
        """Sonucu veritabanına kaydet"""
        try:
            with self.conn:
                self.conn.execute(
                    """INSERT INTO crawled_data 
                    (cve_id, url, html, source, status_code)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        html=excluded.html,
                        source=excluded.source,
                        status_code=excluded.status_code""",
                    (cve_id, url, result["content"], result["source"], result["status"])
                )
            logging.info(f"{Fore.GREEN}[+] {Style.RESET_ALL}Saved: {url}")
        except Exception as e:
            logging.error(f"DB error: {str(e)}")

    async def run(self, dataset_path):
        """Ana çalıştırıcı"""
        with open(dataset_path) as f:
            cve_data = json.load(f)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for entry in cve_data:
                for url in entry['urls']:
                    tasks.append(
                        self.process_url(session, entry['cve_id'], url)
                    )
            await asyncio.gather(*tasks)
        await self.browser.close()
        await self.playwright.stop()
        self.conn.close()

    async def close(self):
        """Kaynakları kapat"""
        await self.browser.close()
        await self.playwright.stop()
        self.conn.close()

async def main():
    crawler = Crawler()
    try:
        await crawler.initialize()
        await crawler.run("..\\dataset.json")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
