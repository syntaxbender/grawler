import psycopg2
import time
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


# Rate limiting kontrolü
last_request_time = time.time()
requests_per_minute = 0
max_requests_per_minute = 60
max_404s_per_minute = 30
error_404s = 0


def rate_limit_check():
    global last_request_time, requests_per_minute, error_404s
    current_time = time.time()
    time_diff = current_time - last_request_time

    # 404 hata sayısının kontrolü
    if error_404s >= max_404s_per_minute:
        print(f"Too many 404s, pausing for 60 seconds.")
        time.sleep(60)  # 404 hataları fazla olduğunda bekleme
        error_404s = 0

    # Eğer 60 istek yapılmışsa, 1 dakika bekle
    if requests_per_minute >= 57:  # 59 isteği tamamladıktan sonra 1 dakika bekle
        wait_time = 60 - time_diff
        print(f"60 requests reached, waiting for {wait_time} seconds.")
        time.sleep(wait_time)
        requests_per_minute = 0  # Bekledikten sonra istek sayısını sıfırla
        last_request_time = time.time()  # Bekleme sonrasında zaman damgasını sıfırla
    else:
        # Rate limit kontrolü yapılmadan önce normal istek işlemine devam et
        requests_per_minute += 1
        last_request_time = time.time()


# Playwright ile URL'den HTML çekme
def fetch_rendered_html(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)  # HTTPS hatalarını yoksay
        page = context.new_page()

        try:
            # Yönlendirme takibi yapılacak şekilde sayfayı yükle
            response = page.goto(url, timeout=10000, wait_until="load")

            # Eğer 302 redirect varsa, yönlendirilen URL'yi al
            redirected_url = response.url if response.status == 302 else url

            # Sayfa tamamen yüklendikten sonra HTML içeriğini al
            page.wait_for_load_state("networkidle", timeout=5000)
            html_content = page.content()

            browser.close()
            return html_content, response.status, redirected_url

        except Exception as e:
            browser.close()
            return None, "Unknown", None


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

    # 302 redirect varsa, yeni URL'ye gidiyoruz
    if status_code == 302:
        redirected_url = error_message  # error_message redirect URL'sini tutar
        print(f"Redirected to: {redirected_url}")
        html_content, status_code, error_message = fetch_rendered_html(redirected_url)

    update_db_with_html(url, html_content, status_code, error_message)


# Ana işlem fonksiyonu
def process_urls():
    conn = init_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT url FROM unique_cve_entries_domain_resolved_true_failed
            WHERE url LIKE '%wordfence.com%' and length(html_content) < 3000
        """)
        urls = [row[0] for row in cursor.fetchall()]
        total_urls = len(urls)
        print(f"Total {total_urls} URLs to process...")

        # ThreadPoolExecutor kullanarak paralel işlemler
        with ThreadPoolExecutor(max_workers=10) as executor:
            for url in urls:
                rate_limit_check()  # Rate limit kontrolü
                executor.submit(process_url, url)

    except Exception as e:
        print(f"Database Query Error: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    process_urls()
