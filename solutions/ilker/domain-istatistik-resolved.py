import psycopg2
from urllib.parse import urlparse
import httpx

# PostgreSQL bağlantısını kurma
def init_db():
    conn = psycopg2.connect(
        host="localhost",
        database="cve_db",
        user="ilker",
        password="123456"
    )
    return conn


def fetch_urls():
    conn = init_db()
    cur = conn.cursor()
    cur.execute("SELECT url FROM unique_cve_entries_domain_resolved_true_failed where length(html_content) < 500")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc


def get_domain_count():
    urls = fetch_urls()
    domain_counts = {}

    for row in urls:
        domain = get_domain(row[0])
        if domain in domain_counts:
            domain_counts[domain] += 1
        else:
            domain_counts[domain] = 1

    # Domain sayımlarını çoktan aza sıralıyoruz
    sorted_domain_counts = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)

    return sorted_domain_counts


async def get_http_status_code(domain):
    async with httpx.AsyncClient() as client:
        try:
            # Domain'e istek gönderiyoruz
            response = await client.get(f"http://{domain}", timeout=10)
            return response.status_code
        except httpx.RequestError as e:
            # Hata durumunda 0 döndürüyoruz (örneğin, bağlantı hatası)
            return 0


async def get_domain_status_codes():
    domain_counts = get_domain_count()

    domain_statuses = {}

    for domain, count in domain_counts:
        status_code = await get_http_status_code(domain)
        domain_statuses[domain] = {
            'count': count,
            'status_code': status_code
        }

    return domain_statuses


# Main fonksiyon, asenkron çalıştırma
import asyncio

async def main():
    domain_statuses = await get_domain_status_codes()
    for domain, data in domain_statuses.items():
        print(f"Domain: {domain}, Count: {data['count']}, Status Code: {data['status_code']}")

# Asenkron çalıştırma
asyncio.run(main())
