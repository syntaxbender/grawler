import time
import requests

# Test parametreleri
base_url = "https://archive.org/wayback/available?url=http://www.securityfocus.com/bid/98519"
test_url_template = "http://web.archive.org/web/20210124053223/https://www.securityfocus.com/bid/98519/"

# İstek hızlarını test etmek için değişkenler
request_intervals = [0.5, 1, 2, 5]  # Saniye cinsinden bekleme süreleri
max_requests = 50  # Maksimum test isteği

def test_rate_limit():
    for interval in request_intervals:
        print(f"\n[*] Testing with interval {interval} seconds")
        success_count = 0
        fail_count = 0

        for i in range(max_requests):
            try:
                # İlk isteği gönder
                response1 = requests.get(base_url)
                if response1.status_code == 200:
                    success_count += 1
                    print(f"[+] {i+1}. request to first URL successful")

                    # İkinci isteği gönder
                    response2 = requests.get(test_url_template)
                    if response2.status_code == 200:
                        print(f"[+] {i+1}. request to second URL successful")
                    else:
                        print(f"[-] {i+1}. request to second URL failed with status {response2.status_code}")
                        fail_count += 1
                else:
                    print(f"[-] {i+1}. request to first URL failed with status {response1.status_code}")
                    fail_count += 1

            except Exception as e:
                print(f"[!] Error on request {i+1}: {str(e)}")
                fail_count += 1

            time.sleep(interval)  # Belirlenen süre kadar bekle

        print(f"[*] Results for interval {interval}: Success {success_count}, Fail {fail_count}")

test_rate_limit()
