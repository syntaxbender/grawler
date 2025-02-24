import psycopg2
import asyncio
import aiohttp
from waybackpy import WaybackMachineCDXServerAPI


# PostgreSQL bağlantısı
def init_db():
    return psycopg2.connect(
        host="localhost",
        database="cve_db",
        user="ilker",
        password="123456"
    )


# Veritabanından çözümlenmemiş ve unique URL'leri al
def get_unresolved_urls():
    conn = init_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT url FROM cve_entries WHERE domain_resolved = false")
    urls = cur.fetchall()
    conn.close()
    return urls  # [(url), (url), ...]


# Veritabanında toplam çözülmemiş URL sayısını al
def get_total_unresolved_urls():
    conn = init_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(DISTINCT url) FROM cve_entries WHERE domain_resolved = false")
    total = cur.fetchone()[0]
    conn.close()
    return total


async def fetch_wayback_content(session, url):
    try:
        wayback = WaybackMachineCDXServerAPI(url)
        snapshots = wayback.snapshots()
        if not snapshots:
            return url, None, "No snapshots found"

        last_snapshot = snapshots[0]  # En yeni arşivlenmiş versiyon (timestamp ile birlikte)
        archive_url = f"http://web.archive.org/web/{last_snapshot}/{url}"  # En yeni snapshot URL'si
        print(f"Çekilen URL: {archive_url}")  # Çekilen URL'i yazdır
        # En son arşivlenmiş içeriği al
        async with session.get(archive_url) as response:
            if response.status == 200:
                return url, await response.text(), None
            else:
                return url, None, f"HTTP {response.status}"

    except Exception as e:
        return url, None, str(e)


# Veritabanını güncelle
def update_database(url, content, error):
    conn = init_db()
    cur = conn.cursor()

    if content:
        # URL'nin bulunduğu tüm satırlarda güncelleme yap
        cur.execute("""
            UPDATE cve_entries 
            SET html_content = %s, status = 'success', domain_resolved = true 
            WHERE url = %s
        """, (content, url))
    else:
        # Hata durumunda ilgili satırları güncelle
        cur.execute("""
            UPDATE cve_entries 
            SET status = 'failed', error_message = %s 
            WHERE url = %s
        """, (error, url))

    conn.commit()
    conn.close()


# Ana işlem fonksiyonu
async def main():
    total_urls = get_total_unresolved_urls()
    urls = get_unresolved_urls()
    processed_urls = 0

    print(f"Total unresolved URLs: {total_urls}")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_wayback_content(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

        # Her URL için güncellemeleri yap
        for url, content, error in results:
            update_database(url, content, error)
            processed_urls += 1
            print(f"Processed {processed_urls}/{total_urls} URLs")


# Çalıştır
if __name__ == "__main__":
    asyncio.run(main())
