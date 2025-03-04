import dns.resolver
import time

servers = ["8.8.8.8","1.1.1.1","8.8.4.4","9.9.9.9","1.0.0.1","208.67.222.222","208.67.220.220","9.9.9.9","149.112.112.112","77.88.8.8","77.88.8.1","37.252.191.197","152.53.15.127","81.169.136.222","51.158.108.203",]
def dns_query(domain):
  for server in servers:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [server]

    start_time = time.time()
    answer = resolver.resolve(domain, "A")  # UDP üzerinden sorgu
    end_time = time.time()

    print(f"")
    for ip in answer:
        print(f"Sorgu süresi: {(end_time - start_time) * 1000:.2f} ms | Yanıt: {ip} | Server: {server}")

if __name__ == "__main__":
    dns_query("syntaxbender.com")
