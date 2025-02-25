import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor
import time


# PostgreSQL bağlantısını kurma
def init_db():
    return psycopg2.connect(
        host="localhost",
        database="cve_db",
        user="ilker",
        password="123456"
    )


# Wayback Machine Availability API'yi kullanarak URL'nin arşivlenip arşivlenmediğini kontrol etme
def check_wayback_availability(url):
    wayback_api_url = f"http://archive.org/wayback/available?url={url}"

    try:
        response = requests.get(wayback_api_url)
        response.raise_for_status()
        data = response.json()

        if "archived_snapshots" in data and "closest" in data["archived_snapshots"]:
            snapshots = data["archived_snapshots"]
            if snapshots:
                sorted_snapshots = sorted(snapshots.values(), key=lambda x: x['timestamp'])
                first_snapshot = sorted_snapshots[0]
                if first_snapshot["available"]:
                    return first_snapshot["url"]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error checking Wayback availability for {url}: {e}")
        return None


# Wayback Machine'den HTML içeriğini çekme
def fetch_wayback_html(url):
    archived_url = check_wayback_availability(url)
    if archived_url:
        try:
            response = requests.get(archived_url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching content from Wayback Machine: {e}")
    return None


# Veritabanına HTML içeriğini kaydetme
def update_db_with_html(url, html_content):
    conn = init_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE unique_cve_entries_domain_resolved_false
            SET html_content = %s, status = 'success'
            WHERE url = %s
        """, (html_content, url))
        conn.commit()
    except Exception as e:
        print(f"Error updating the database: {e}")
    finally:
        cursor.close()
        conn.close()


# Ana işlem fonksiyonu
def process_url(url):
    print(f"Processing {url}")
    html_content = fetch_wayback_html(url)
    if html_content:
        update_db_with_html(url, html_content)
    else:
        print(f"No content found for {url}")


# İlerleme takibi ve paralel işlem için ana fonksiyon
def main():
    conn = init_db()
    cursor = conn.cursor()

    try:
        # Sadece html_content IS NULL olan kayıtları al
        cursor.execute("""
            SELECT url FROM unique_cve_entries_domain_resolved_false
            WHERE html_content IS NULL
              AND url NOT LIKE 'file://%'
              AND url NOT LIKE 'ftp://%'
        """)
        rows = cursor.fetchall()

        total_urls = len(rows)
        print(f"Total {total_urls} URLs to process")

        with ThreadPoolExecutor(max_workers=3) as executor:
            for index, row in enumerate(rows, start=1):
                url = row[0]
                print(f"Processing {index}/{total_urls} - {url}")
                executor.submit(process_url, url)
                time.sleep(1)  # Rate limit için bekleme

    except Exception as e:
        print(f"Error fetching data from the database: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
