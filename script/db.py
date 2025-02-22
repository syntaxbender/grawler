import asyncio
import aiohttp
import sqlite3
import json
import sys
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from waybackpy import WaybackMachineCDXServerAPI
import os

DB_FILE = "crawled_data.db"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_CONCURRENT_REQUESTS = 50  # Sınırlı eşzamanlı istek
columns, rows = os.get_terminal_size()

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Eşzamanlı istekleri sınırla


def log(message, color="white"):
    """Renkli log çıktısı"""
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "white": "\033[97m"
    }
    reset = "\033[0m"
    print(f"{colors.get(color, colors['white'])}[LOG] {message}{reset}")

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
        response = await page.goto(url, timeout=15000)
        
        if response and response.status == 202:
            log(f"HTTP 202 tespit edildi! Bekleniyor... {url}", "yellow")
            await page.wait_for_timeout(3000)  # 3 saniye bekle
            redirected_url = page.url  # Yeni yönlendirilen URL'yi al
            log("✓" * columns, "yellow")
            log(f"Yönlendirilen yeni URL: {redirected_url}", "white")
            log("✓" * columns, "yellow")
            await page.goto(redirected_url, timeout=15000)  # Yeni sayfayı aç
        
        content = await page.content()
        log(f"Fetched JS content from {url}")
        return content
    except Exception as e:
        log(f"Failed to fetch JS content from {url}: {e}", "red")
        return None

async def fetch_static_html(session, url):
    """JavaScript içermeyen sayfalar için aiohttp ile HTML getirir."""
    async with semaphore:  # Eşzamanlı istekleri kontrol et
        try:
            async with session.get(url, headers=HEADERS, timeout=10) as resp:
                if resp.status == 200:
                    log(f"Fetched static HTML from {url}", "green")
                    return await resp.text()
                elif resp.status == 202:
                    log(f"HTTP 202 alındı! Playwright ile işlenmeli: {url}", "yellow")
                    return None  # Playwright ile işlenecek
                else:
                    log(f"Failed to fetch static HTML from {url}: HTTP {resp.status}", "red")
                    return None
        except Exception as e:
            log(f"Error fetching static HTML from {url}: {e}", "red")
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
                    log(f"Recovered content from Wayback Machine for {url}", "green")
                return content
    except Exception as e:
        log(f"Failed to recover from Wayback Machine for {url}: {e}", "red")
        return None

async def process_url(session, page, cve_id, url):
    """Tek bir URL işleyerek HTML verisini alır ve kaydeder."""
    html = await fetch_static_html(session, url)
    
    if not html:
        html = await fetch_html(page, url)  # Playwright ile dene
    
    if not html:
        html = await recover_from_wayback(url)  # Wayback Machine'den al

    if html:
        save_to_db(cve_id, url, html)
    else:
        log(f"No content available for {url}", "red")

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
        log(f"Saved {url} to database", "blue")

if __name__ == "__main__":
    log("Initializing database...", "blue")
    init_db()
    log("Starting URL processing...", "blue")
    
    with open("..\\dataset.json", "r") as file:
        columns, rows = os.get_terminal_size()
        cve_data = json.load(file)

    asyncio.run(process_batch(cve_data))

    log("Processing complete!", "green")
