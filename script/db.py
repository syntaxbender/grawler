
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
