import requests
import time
def doh_query(domain, record_type="A"):
    url = "https://cloudflare-dns.com/dns-query"
    params = {"name": domain, "type": record_type}
    headers = {"Accept": "application/dns-json"}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}, {response.text}"

# Örnek kullanım
start_time = time.time()  # Sorgu başlangıç zamanı
result = doh_query("vireo.software")
end_time = time.time()  # Sorgu bitiş zamanı
elapsed_time = (end_time - start_time) * 1000
print(f"Sorgu süresi: {elapsed_time:.2f} ms\n")


print(result)
