# from __future__ import annotations

# import asyncio
# import json
# import logging
# import sqlite3
# from datetime import datetime
# from pathlib import Path
# from typing import Any, AsyncGenerator, Dict, List, Optional

# import aiohttp
# import httpx
# from fastapi.encoders import jsonable_encoder
# from playwright.async_api import async_playwright
# from pydantic import BaseModel, Field
# from rich.console import Console
# from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
# from waybackpy import WaybackMachineCDX

# # Rich console için setup
# console = Console()

# class URLContent(BaseModel):
#     """URL içeriği için Pydantic model"""
#     cve_id: str
#     url: str
#     html_content: Optional[str] = None
#     is_active: bool = False
#     source: str = Field(default="direct", description="direct, wayback, or failed")
#     processed_at: datetime = Field(default_factory=datetime.now)
#     error_message: Optional[str] = None

# class CVEData(BaseModel):
#     """CVE verisi için Pydantic model"""
#     cve_id: str
#     urls: List[str]

# class DatabaseManager:
#     def __init__(self, db_path: str | Path):
#         self.db_path = Path(db_path)
#         self.init_db()

#     def init_db(self) -> None:
#         with sqlite3.connect(self.db_path) as conn:
#             conn.execute("""
#                 CREATE TABLE IF NOT EXISTS url_contents (
#                     id INTEGER PRIMARY KEY AUTOINCREMENT,
#                     cve_id TEXT NOT NULL,
#                     url TEXT NOT NULL,
#                     html_content TEXT,
#                     is_active BOOLEAN,
#                     source TEXT,
#                     processed_at TEXT,
#                     error_message TEXT,
#                     UNIQUE(cve_id, url)
#                 )
#             """)

#     async def save_url_data(self, data: URLContent) -> None:
#         with sqlite3.connect(self.db_path) as conn:
#             conn.execute("""
#                 INSERT OR REPLACE INTO url_contents 
#                 (cve_id, url, html_content, is_active, source, processed_at, error_message)
#                 VALUES (?, ?, ?, ?, ?, ?, ?)
#             """, (
#                 data.cve_id,
#                 data.url,
#                 data.html_content,
#                 data.is_active,
#                 data.source,
#                 data.processed_at.isoformat(),
#                 data.error_message
#             ))

# class URLCrawler:
#     def __init__(self, db_manager: DatabaseManager):
#         self.db_manager = db_manager
#         self.logger = self._setup_logger()
#         self.httpx_client: Optional[httpx.AsyncClient] = None
#         self.browser = None

#     @staticmethod
#     def _setup_logger() -> logging.Logger:
#         logger = logging.getLogger("URLCrawler")
#         logger.setLevel(logging.INFO)
        
#         # Rich handler kullanarak renkli ve formatlı loglar
#         handler = logging.StreamHandler()
#         formatter = logging.Formatter(
#             '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#         )
#         handler.setFormatter(formatter)
#         logger.addHandler(handler)
#         return logger

#     async def __aenter__(self) -> URLCrawler:
#         self.httpx_client = httpx.AsyncClient(
#             timeout=30.0,
#             follow_redirects=True,
#             http2=True
#         )
#         playwright = await async_playwright().start()
#         self.browser = await playwright.chromium.launch(
#             headless=True
#         )
#         return self

#     async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
#         if self.httpx_client:
#             await self.httpx_client.aclose()
#         if self.browser:
#             await self.browser.close()

#     async def fetch_with_playwright(self, url: str) -> Optional[str]:
#         try:
#             page = await self.browser.new_page()
#             response = await page.goto(url, wait_until="networkidle", timeout=30000)
            
#             if not response:
#                 return None
                
#             if response.status == 200:
#                 content = await page.content()
#                 await page.close()
#                 return content
            
#             await page.close()
#             return None
            
#         except Exception as e:
#             self.logger.error(f"Playwright error for {url}: {str(e)}")
#             return None

#     async def fetch_with_httpx(self, url: str) -> Optional[str]:
#         try:
#             if not self.httpx_client:
#                 return None
                
#             response = await self.httpx_client.get(url)
#             if response.status_code == 200:
#                 return response.text
#             return None
            
#         except Exception as e:
#             self.logger.error(f"HTTPX error for {url}: {str(e)}")
#             return None

#     async def fetch_from_wayback(self, url: str) -> Optional[str]:
#         try:
#             cdx = WaybackMachineCDX(url)
#             newest = cdx.newest()
#             if newest and self.httpx_client:
#                 response = await self.httpx_client.get(newest.archive_url)
#                 if response.status_code == 200:
#                     return response.text
#             return None
            
#         except Exception as e:
#             self.logger.error(f"Wayback error for {url}: {str(e)}")
#             return None

#     async def process_url(self, cve_id: str, url: str) -> URLContent:
#         url_data = URLContent(
#             cve_id=cve_id,
#             url=url,
#             processed_at=datetime.now()
#         )

#         try:
#             # İlk olarak HTTPX ile dene
#             content = await self.fetch_with_httpx(url)
            
#             # Başarısız olursa Playwright ile dene
#             if not content:
#                 content = await self.fetch_with_playwright(url)

#             if content:
#                 url_data.html_content = content
#                 url_data.is_active = True
#                 url_data.source = "direct"
#             else:
#                 # Son olarak Wayback Machine'i dene
#                 wayback_content = await self.fetch_from_wayback(url)
#                 if wayback_content:
#                     url_data.html_content = wayback_content
#                     url_data.source = "wayback"
#                 else:
#                     url_data.source = "failed"
#                     url_data.error_message = "Could not fetch content from any source"

#         except Exception as e:
#             url_data.source = "failed"
#             url_data.error_message = str(e)

#         await self.db_manager.save_url_data(url_data)
#         return url_data

#     async def process_cve_batch(
#         self,
#         cve_data: Dict[str, List[str]],
#         batch_size: int = 10
#     ) -> AsyncGenerator[URLContent, None]:
#         with Progress(
#             SpinnerColumn(),
#             TextColumn("[progress.description]{task.description}"),
#             TimeElapsedColumn(),
#             console=console
#         ) as progress:
#             task = progress.add_task("Processing URLs...", total=len(cve_data))
            
#             for cve_id, urls in cve_data.items():
#                 tasks = []
#                 for url in urls:
#                     if len(tasks) >= batch_size:
#                         results = await asyncio.gather(*tasks)
#                         for result in results:
#                             yield result
#                         tasks = []
                        
#                     tasks.append(self.process_url(cve_id, url))
                
#                 if tasks:
#                     results = await asyncio.gather(*tasks)
#                     for result in results:
#                         yield result
                        
#                 progress.update(task, advance=1)

# async def load_json_data(file_path: str | Path) -> Dict[str, List[str]]:
#     """JSON dosyasından CVE verilerini yükle"""
#     file_path = Path(file_path)
#     if not file_path.exists():
#         raise FileNotFoundError(f"JSON file not found: {file_path}")
        
#     with open(file_path, 'r') as f:
#         data = json.load(f)
#     return data

# async def main():
#     # JSON dosyasından veriyi yükle
#     try:
#         data = await load_json_data('dataset.json')
#     except Exception as e:
#         console.print(f"[red]Error loading JSON data: {e}[/red]")
#         return

#     db_manager = DatabaseManager('cve_urls.db')
    
#     async with URLCrawler(db_manager) as crawler:
#         results = []
#         async for result in crawler.process_cve_batch(data):
#             results.append(jsonable_encoder(result))
            
#         # Sonuçları JSON olarak kaydet
#         with open('results.json', 'w') as f:
#             json.dump(results, f, indent=2)
            
#         console.print("[green]Crawling completed! Results saved to results.json[/green]")

# if __name__ == "__main__":
#     asyncio.run(main())

import asyncio
import aiohttp
import sqlite3
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from waybackpy import WaybackMachineCDXServerAPI

DB_FILE = "crawled_data.db"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_CONCURRENT_REQUESTS = 50  # Sınırlı eşzamanlı istek

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Eşzamanlı istekleri sınırla

def log(message):
    print(f"[LOG] {message}")

def init_db():
    """Veritabanını başlatır."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawled_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cve_id TEXT,
                url TEXT,
                html TEXT
            )"""
        )
        conn.commit()

async def fetch_html(page, url):
    """JavaScript içeren sayfalar için Playwright ile HTML getirir."""
    try:
        await page.goto(url, timeout=15000)
        content = await page.content()
        log(f"Fetched JS content from {url}")
        return content
    except Exception as e:
        log(f"Failed to fetch JS content from {url}: {e}")
        return None

async def fetch_static_html(session, url):
    """JavaScript içermeyen sayfalar için aiohttp ile HTML getirir."""
    async with semaphore:  # Eşzamanlı istekleri kontrol et
        try:
            async with session.get(url, headers=HEADERS, timeout=10) as resp:
                if resp.status == 200:
                    log(f"Fetched static HTML from {url}")
                    return await resp.text()
                else:
                    log(f"Failed to fetch static HTML from {url}: HTTP {resp.status}")
                    return None
        except Exception as e:
            log(f"Error fetching static HTML from {url}: {e}")
            return None

async def recover_from_wayback(url):
    """Wayback Machine’den veri alır."""
    try:
        cdx = WaybackMachineCDXServerAPI(url)
        snapshots = cdx.snapshots()
        if snapshots:
            latest_snapshot = snapshots[-1]["url"]
            async with aiohttp.ClientSession() as session:
                content = await fetch_static_html(session, latest_snapshot)
                if content:
                    log(f"Recovered content from Wayback Machine for {url}")
                return content
    except Exception as e:
        log(f"Failed to recover from Wayback Machine for {url}: {e}")
        return None

async def process_url(session, page, cve_id, url):
    """Tek bir URL işleyerek HTML verisini alır ve kaydeder."""
    html = await fetch_static_html(session, url) or await fetch_html(page, url)
    if not html:
        html = await recover_from_wayback(url)
    if html:
        save_to_db(cve_id, url, html)
    else:
        log(f"No content available for {url}")

async def process_batch(cve_data):
    """Tüm URL'leri asenkron şekilde işler."""
    async with aiohttp.ClientSession() as session:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            tasks = [process_url(session, page, item['cve_id'], url) 
                     for item in cve_data for url in item['urls']]
            await asyncio.gather(*tasks)
            
            await browser.close()

def save_to_db(cve_id, url, html):
    """Veriyi SQLite veritabanına kaydeder."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO crawled_data (cve_id, url, html) VALUES (?, ?, ?)", (cve_id, url, html))
        conn.commit()
        log(f"Saved {url} to database")

if __name__ == "__main__":
    log("Initializing database...")
    init_db()
    log("Starting URL processing...")
    with open("dataset.json", "r") as file:
        cve_data = json.load(file)
    asyncio.run(process_batch(cve_data))
    log("Processing complete!")
