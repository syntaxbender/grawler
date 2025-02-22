from fake_useragent import UserAgent
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

ua = UserAgent()

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

async def fetch_html(browser, url):
    """JavaScript içeren sayfalar için Playwright ile HTML getirir."""
    try:
        page = await browser.new_page()
        response = await page.goto(url, timeout=30000)
        
        if response and response.status == 202:
            log(f"HTTP 202 tespit edildi! Bekleniyor... {url}", "yellow")
            await page.wait_for_timeout(3000)  # 3 saniye bekle
            redirected_url = page.url  # Yeni yönlendirilen URL'yi al
            log("✓" * columns, "yellow")
            log(f"Yönlendirilen yeni URL: {redirected_url}", "white")
            log("✓" * columns, "yellow")
            await page.goto(redirected_url, timeout=30000)  # Yeni sayfayı aç
        
        content = await page.content()
        await page.close()
        log(f"Fetched JS content from {url}")
        return content
    except Exception as e:
        log(f"Failed to fetch JS content from {url}: {e}", "red")
        return None

async def fetch_static_html(session, url):
    """JavaScript içermeyen sayfalar için aiohttp ile HTML getirir."""
    async with semaphore:  # Eşzamanlı istekleri kontrol et
        try:
            async with session.get(url, headers=HEADERS, timeout=30, ssl=False) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    
                    # Eğer içerik PDF ise kaydet ve işleme alma
                    if "application/pdf" in content_type:
                        log(f"PDF dosyası bulundu, kaydediliyor: {url}", "blue")
                        pdf_content = await resp.read()
                        filename = url.split("/")[-1]
                        with open(filename, "wb") as f:
                            f.write(pdf_content)
                        return None  # PDF içeriği HTML gibi işlenmesin
                    
                    log(f"Fetched static HTML from {url}", "green")
                    return await resp.text()
                
                elif resp.status in [403, 503]:
                    log(f"Site botları engelliyor. Playwright denenecek: {url}", "yellow")
                    return None  # Playwright ile denenecek
                
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

async def process_url(session, browser, cve_id, url):
    """Tek bir URL işleyerek HTML verisini alır ve kaydeder."""
    html = await fetch_static_html(session, url)
    
    if not html:
        html = await fetch_html(browser, url)  # Playwright ile dene
    
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
            
            tasks = [process_url(session, browser, item['cve_id'], url) 
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
        HEADERS = {"User-Agent": ua.random}
        columns, rows = os.get_terminal_size()
        cve_data = json.load(file)

    asyncio.run(process_batch(cve_data))

    log("Processing complete!", "green")
