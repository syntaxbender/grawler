import psycopg2
import threading
import random
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright

# PostgreSQL bağlantısını kurma
def init_db():
    return psycopg2.connect(
        host="localhost",
        database="cve_db",
        user="ilker",
        password="123456"
    )

# Playwright ile URL'den HTML çekme
def fetch_rendered_html(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)  # HTTPS hatalarını yoksay
        page = context.new_page()

        try:
            # HTTP/2'yi devre dışı bırak
            context.set_extra_http_headers({"Upgrade-Insecure-Requests": "1"})

            response = page.goto(url, timeout=10000, wait_until="load")
            status_code = response.status if response else "Unknown"

            page.wait_for_load_state("networkidle", timeout=5000)

            html_content = page.content()
            browser.close()
            return html_content, status_code, None

        except Exception as e:
            browser.close()
            return None, "Unknown", str(e)

# Veritabanına güncelleme yapma
def update_db_with_html(url, html_content, status_code, error_message):
    conn = init_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE unique_cve_entries_domain_resolved_true_failed
            SET html_content = %s, status = %s, error_message = %s, last_updated = NOW()
            WHERE url = %s
        """, (html_content, "success" if html_content else "failed", error_message, url))
        conn.commit()
    except Exception as e:
        print(f"DB Update Error for {url}: {e}")
    finally:
        cursor.close()
        conn.close()

# URL işleme fonksiyonu (Her URL için paralel işlem)
def process_url(url):
    print(f"Fetching: {url}")
    html_content, status_code, error_message = fetch_rendered_html(url)
    update_db_with_html(url, html_content, status_code, error_message)

# Ana işlem fonksiyonu
def process_urls():
    conn = init_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT url FROM unique_cve_entries_domain_resolved_true_failed
            WHERE html_content IS NULL
              AND url NOT LIKE 'file://%'
              AND url NOT LIKE 'ftp://%'
              AND url NOT LIKE '%.pdf%'
              AND url NOT LIKE '%.docx%'
              AND url NOT LIKE '%.zip%'
              AND url NOT LIKE '%osvdb%'
            ORDER BY RANDOM()  -- Rastgele sırayla seç
        """)
        urls = [row[0] for row in cursor.fetchall()]
        total_urls = len(urls)
        print(f"Total {total_urls} URLs to process...")

        # ThreadPoolExecutor kullanarak paralel işlemler
        with ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(process_url, urls)

    except Exception as e:
        print(f"Database Query Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    process_urls()
