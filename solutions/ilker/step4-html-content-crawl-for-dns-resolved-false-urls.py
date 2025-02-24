import psycopg2
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time  # time modülünü ekliyoruz


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
        response.raise_for_status()  # Hata kontrolü yapar
        data = response.json()

        if "archived_snapshots" in data and "closest" in data["archived_snapshots"]:
            snapshots = data["archived_snapshots"]
            if snapshots:
                # SnapShotları tarihe göre sıralayıp en eskiyi seçelim
                sorted_snapshots = sorted(snapshots.values(), key=lambda x: x['timestamp'])
                first_snapshot = sorted_snapshots[0]  # En eski snapshot
                if first_snapshot["available"]:
                    return first_snapshot["url"]  # Arşivlenmiş içerik varsa URL'yi döndürür
                else:
                    print(f"No archived content found for {url}")
                    return None
            else:
                print(f"No archived content found for {url}")
                return None
        else:
            print(f"No archived content found for {url}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error checking Wayback availability for {url}: {e}")
        return None


# Wayback Machine'den HTML içeriğini çekme
def fetch_wayback_html(url):
    archived_url = check_wayback_availability(url)
    if archived_url:
        try:
            # Arşivlenmiş URL'yi kullanarak HTML içeriğini alıyoruz
            response = requests.get(archived_url)
            response.raise_for_status()  # Hata kontrolü yapar
            return response.text  # HTML içeriği döndürüyoruz
        except requests.exceptions.RequestException as e:
            print(f"Error fetching content from Wayback Machine: {e}")
            return None
    else:
        return None


# Veritabanına HTML içeriğini kaydetme
def update_db_with_html(url, html_content):
    conn = init_db()
    cursor = conn.cursor()

    try:
        # HTML içeriğini ve durumu güncelleme
        cursor.execute("""
            UPDATE cve_entries
            SET html_content = %s, status = 'success'
            WHERE url = %s AND domain_resolved = false
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
        # HTML içeriğini veritabanına kaydet
        update_db_with_html(url, html_content)
    else:
        print(f"No content found for {url}")


# İlerleme takibi ve paralel işlem için ana fonksiyon
def main():
    conn = init_db()
    cursor = conn.cursor()

    try:
        # domain_resolved false olan ve unique URL'leri seç
        cursor.execute("""
            SELECT DISTINCT url FROM cve_entries
            WHERE domain_resolved = false
              AND url NOT LIKE 'file://%'  -- 'file' ile başlayanları filtrele
              AND url NOT LIKE 'ftp://%'   -- 'ftp' ile başlayanları filtrele
        """)
        rows = cursor.fetchall()

        total_urls = len(rows)  # Toplam URL sayısını al
        print(f"Total {total_urls} URLs to process")

        # Paralel işlem için ThreadPoolExecutor kullanma
        with ThreadPoolExecutor(max_workers=2) as executor:
            for index, row in enumerate(rows, start=1):
                url = row[0]
                # İlerleme yazdırma
                print(f"Processing {index}/{total_urls} - {url}")
                # URL'yi paralel olarak işleme
                executor.submit(process_url, url)

                # Rate limit için bekleme
                time.sleep(1)  # Her istek arasında 1 saniye bekleme

    except Exception as e:
        print(f"Error fetching data from the database: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
