import psycopg2
import aiohttp
import asyncio
from waybackpy import WaybackMachineCDXServerAPI


# PostgreSQL bağlantısını kurma
def init_db():
    conn = psycopg2.connect(
        host="localhost",
        database="cve_db",
        user="ilker",
        password="123456"
    )
    cur = conn.cursor()

    # Tabloyu oluşturma
    cur.execute('''
        CREATE TABLE IF NOT EXISTS waybackmachine_results (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            waybackmachine_url TEXT NOT NULL,
            html_content TEXT NOT NULL,
            status TEXT NOT NULL
        )
    ''')
    conn.commit()
    return conn


# URL'leri almak için fonksiyon
def fetch_urls():
    conn = init_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM unique_cve_entries_domain_resolved_false WHERE url NOT LIKE 'file://%' AND url NOT LIKE 'ftp://%'")
    rows = cur.fetchall()
    conn.close()
    return rows


# Asenkron şekilde Wayback Machine'den içerik çekme
async def fetch_wayback_snapshot(url, semaphore):
    async with semaphore:
        try:
            conn = init_db()
            cur = conn.cursor()

            # Veritabanında URL zaten mevcutsa kaydetme
            cur.execute("SELECT * FROM waybackmachine_results WHERE url = %s and status = 'success'", (url,))
            row = cur.fetchone()
            if row:
                print(f'Skipping {url} as it has already been saved')
                conn.close()
                return

            cdx = WaybackMachineCDXServerAPI(url)
            if ss := cdx.oldest():
                async with aiohttp.ClientSession() as session:
                    async with session.get(ss.archive_url) as resp:
                        if resp.status == 200:
                            print(f'Fetching {ss.archive_url} for {url}')
                            content = await resp.text()

                            # Veritabanına kaydetme
                            cur.execute('''
                                INSERT INTO waybackmachine_results (url, waybackmachine_url, html_content, status)
                                VALUES (%s, %s, %s, %s)
                            ''', (url, ss.archive_url, content, 'success'))
                            conn.commit()
                        else:
                            # Hata durumunda kaydetme
                            cur.execute('''
                                INSERT INTO waybackmachine_results (url, waybackmachine_url, html_content, status)
                                VALUES (%s, %s, %s, %s)
                            ''', (url, ss.archive_url, '', f'failed: {resp.status} {resp.reason}'))
                            conn.commit()
        except Exception as e:
            print(f'Error fetching {url}: {str(e)}')
            cur.execute('''
                INSERT INTO waybackmachine_results (url, waybackmachine_url, html_content, status)
                VALUES (%s, %s, %s, %s)
            ''', (url, '', '', f'error: {str(e)}'))
            conn.commit()
        finally:
            conn.close()


# Asenkron olarak URL'leri işlemek için fonksiyon
async def process_chunk(chunk, semaphore):
    tasks = [fetch_wayback_snapshot(row[2], semaphore) for row in chunk]  # row[2] -> url column
    await asyncio.gather(*tasks)


# Ana işlev
async def main():
    urls = fetch_urls()
    semaphore = asyncio.Semaphore(5)  # Aynı anda 5 istek yapalım

    chunk_size = len(urls) // 10  # URL'leri 10 parçaya bölelim
    tasks = []
    for i in range(0, len(urls), chunk_size):
        chunk = urls[i:i + chunk_size]
        tasks.append(asyncio.create_task(process_chunk(chunk, semaphore)))

    await asyncio.gather(*tasks)


# Ana işlevi çalıştırma
asyncio.run(main())
