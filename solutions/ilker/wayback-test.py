import requests


def check_wayback_availability(url):
    wayback_api_url = f"http://archive.org/wayback/available?url={url}"

    try:
        response = requests.get(wayback_api_url)
        response.raise_for_status()  # Hata kontrolü yapar
        data = response.json()

        if "archived_snapshots" in data and "closest" in data["archived_snapshots"]:
            snapshot = data["archived_snapshots"]["closest"]
            if snapshot["available"]:
                print(f"Content found for {url}: {snapshot['url']}")
            else:
                print(f"No archived content found for {url}")
        else:
            print(f"No archived content found for {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error checking Wayback availability for {url}: {e}")


# Kullanım örneği
urls = [
    "http://www.z0rlu.ownspace.org/index.php?/archives/74-Powered-by-TLM-CMS-index.php-sql-inj..html",
    "http://www.z0rlu.ownspace.org/index.php?/archives/75-GEDCOM_to_MySQL2-XSS.html",
    "http://www.z0rlu.ownspace.org/index.php?/archives/84-ACGV-News-v0.9.1-2003-SQL-inj.-XSS.html",
    "http://www.securityfocus.com/bid/98519"
]

for url in urls:
    check_wayback_availability(url)

