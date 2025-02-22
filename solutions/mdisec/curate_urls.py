import json
from urllib.parse import urlparse
import socket
import asyncio
import aiohttp
import aiofiles
import requests
from waybackpy import WaybackMachineCDXServerAPI
from playwright.async_api import async_playwright
import os
import aiosqlite
import re

# Load the JSON data from the file
with open('small_sample_data.json', 'r') as file:
    data = json.load(file)

# Extract URLs and create a unique list
unique_urls = set()
for entry in data:
    for url in entry['urls']:
        unique_urls.add(url)

# Convert the set back to a list
unique_urls = list(unique_urls)

# Extract domain names and create a unique list
unique_domains = set()
for url in unique_urls:
    domain = urlparse(url).netloc
    unique_domains.add(domain)

# Convert the set back to a list
unique_domains = list(unique_domains)

# Asynchronous function to check if a domain has a resolution
async def check_domain_resolution(domain):
    loop = asyncio.get_event_loop()
    try:
        await loop.getaddrinfo(domain, None, proto=socket.IPPROTO_TCP)
        return domain
    except socket.gaierror:
        return None

# Asynchronous function to fetch the latest snapshot from the Wayback Machine
async def fetch_wayback_snapshot(url):
    try:
        cdx = WaybackMachineCDXServerAPI(url)
        if ss := cdx.newest():
            async with aiohttp.ClientSession() as session:
                async with session.get(ss.archive_url) as resp:
                    if resp.status == 200:
                        return await resp.text(), None
                    else:
                        return None, {"url": url, "status": resp.status, "reason": resp.reason}
    except Exception as e:
        return None, {"url": url, "status": "error", "reason": str(e)}

# Asynchronous function to fetch the HTML content of a URL using Playwright with a single browser instance
async def fetch_html_content_with_playwright(url, browser):
    try:
        page = await browser.new_page()
        response = await page.goto(url)
        if response.status != 200:
            print(f"Failed to load {url}: {response.status}")
            return None, {"url": url, "status": response.status, "reason": response.status_text}
        content = await page.content()
        await page.close()
        return content, None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None, {"url": url, "status": "error", "reason": str(e)}

# Ensure the results directory exists
os.makedirs('results', exist_ok=True)

# Function to initialize the SQLite database
async def init_db(db_path='results/urls.db'):
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                title TEXT,
                content TEXT NOT NULL
            )
        ''')
        await db.commit()

# Function to save content to the SQLite database
async def save_content_to_db(url, content, db_path='results/urls.db'):
    try:
        title = extract_title(content)
        async with aiosqlite.connect(db_path) as db:
            await db.execute('''
                INSERT INTO urls (url, title, content) VALUES (?, ?, ?)
            ''', (url, title, content))
            await db.commit()
    except Exception as e:
        print(f"Error saving content for {url}: {e}")

# Function to extract the title from the HTML content using regex
def extract_title(content):
    match = re.search(r'<title>(.*?)</title>', content, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return None

# Function to save failed URLs to a JSON file
async def save_failed_urls(failed_urls, folder='results'):
    try:
        filename = os.path.join(folder, 'failed.json')
        async with aiofiles.open(filename, 'w') as file:
            await file.write(json.dumps(failed_urls, indent=4))
    except Exception as e:
        print(f"Error saving failed URLs: {e}")

# Function to save results to the database
async def save_results_to_db(resolved_urls, html_contents, unresolved_urls, wayback_urls, fetch_wayback_for_unresolved, db_path='results/urls.db'):
    for url, content in zip(resolved_urls, html_contents):
        if content:
            await save_content_to_db(url, content, db_path)

    if fetch_wayback_for_unresolved:
        for url, content in zip(unresolved_urls, wayback_urls):
            if content:
                await save_content_to_db(url, content, db_path)

# Main function to check all domains and fetch Wayback Machine snapshots
async def main(fetch_wayback_for_unresolved=True):
    await init_db()
    resolved_domains = await resolve_domains(unique_domains)
    resolved_urls, unresolved_urls = filter_urls_by_resolution(unique_urls, resolved_domains)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        html_contents, failed_urls = await fetch_html_contents(resolved_urls, browser)
        await browser.close()

    wayback_urls = []
    if fetch_wayback_for_unresolved:
        wayback_urls, wayback_failed_urls = await fetch_wayback_snapshots(unresolved_urls)
        failed_urls.extend(wayback_failed_urls)

    await save_results_to_db(resolved_urls, html_contents, unresolved_urls, wayback_urls, fetch_wayback_for_unresolved)
    await save_failed_urls(failed_urls)

async def resolve_domains(domains):
    tasks = [check_domain_resolution(domain) for domain in domains]
    resolved_domains = await asyncio.gather(*tasks)
    return [domain for domain in resolved_domains if domain]

def filter_urls_by_resolution(urls, resolved_domains):
    resolved_urls = [url for url in urls if urlparse(url).netloc in resolved_domains]
    unresolved_urls = [url for url in urls if urlparse(url).netloc not in resolved_domains]
    return resolved_urls, unresolved_urls

async def fetch_html_contents(urls, browser):
    tasks = [fetch_html_content_with_playwright(url, browser) for url in urls]
    results = await asyncio.gather(*tasks)
    html_contents = [result[0] for result in results]
    failed_urls = [result[1] for result in results if result[1]]
    return html_contents, failed_urls

async def fetch_wayback_snapshots(urls):
    tasks = [fetch_wayback_snapshot(url) for url in urls]
    results = await asyncio.gather(*tasks)
    wayback_urls = [result[0] for result in results]
    failed_urls = [result[1] for result in results if result[1]]
    return wayback_urls, failed_urls

# Run the main function with the option to fetch Wayback Machine content for unresolved URLs
asyncio.run(main(fetch_wayback_for_unresolved=False))