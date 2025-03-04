import socket
import struct
import time
def build_dns_query(domain):
    # 16-bit işlem ID (örneğin, 0x1234)
    transaction_id = b'\x12\x34'
    # Flags: Standart sorgu, recursion istekli (0x0100)
    flags = b'\x01\x00'
    # Soru sayısı: 1
    qdcount = b'\x00\x01'
    # Cevap, yetki ve ek bilgi kayıtları: 0
    ancount = b'\x00\x00'
    nscount = b'\x00\x00'
    arcount = b'\x00\x00'
    
    header = transaction_id + flags + qdcount + ancount + nscount + arcount
    
    # Domain adını QNAME formatında oluşturuyoruz
    qname = b''
    for label in domain.split('.'):
        length = len(label)
        qname += bytes([length]) + label.encode('utf-8')
    qname += b'\x00'  # Domain sonlandırma byte'ı

    # QTYPE: A kaydı (IPv4 adresi) => 0x0001
    qtype = b'\x00\x01'
    # QCLASS: IN (Internet) => 0x0001
    qclass = b'\x00\x01'
    
    query = qname + qtype + qclass
    return header + query

def send_dns_query(server, domain):
    # DNS sorgu paketini oluştur
    query_packet = build_dns_query(domain)
    
    # UDP soketi oluştur ve zaman aşımını ayarla
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)
    
    # DNS sunucusuna (1.1.1.1, port 53) sorguyu gönder
    server_address = (server, 53)
    sock.sendto(query_packet, server_address)
    
    # Yanıtı al (maksimum 512 bayt)
    response, _ = sock.recvfrom(512)
    sock.close()
    return response

def parse_dns_response(response):
    # İlk 12 byte başlıktır
    transaction_id = response[:2]
    flags = response[2:4]
    qdcount = struct.unpack("!H", response[4:6])[0]
    ancount = struct.unpack("!H", response[6:8])[0]

    print(f"Transaction ID: {transaction_id.hex()}")
    print(f"Flags: {flags.hex()}")
    print(f"QDCOUNT: {qdcount}, ANCOUNT: {ancount}")

    # 12. bayttan itibaren gelen domain adı çözülmeli
    index = 12
    domain_parts = []
    while True:
        length = response[index]
        if length == 0:
            index += 1
            break
        domain_parts.append(response[index+1:index+1+length].decode())
        index += length + 1
    queried_domain = ".".join(domain_parts)
    print(f"Sorgulanan Alan Adı: {queried_domain}")

    # Cevap Bölümünü Ayrıştır
    if ancount > 0:
        index += 4  # QTYPE ve QCLASS'ı atlıyoruz
        print("Yanıtlar:")
        for _ in range(ancount):
            # Yanıttaki Name alanını oku (genellikle sıkıştırma kullanılır)
            name_pointer = struct.unpack("!H", response[index:index+2])[0]
            index += 2
            rtype, rclass, ttl, rdlength = struct.unpack("!HHIH", response[index:index+10])
            index += 10
            if rtype == 1:  # A kaydı (IPv4)
                ip_address = ".".join(map(str, response[index:index+rdlength]))
                print(f"  - IPv4 Adresi: {ip_address}")
            index += rdlength
    else:
        print("Yanıt bulunamadı.")

if __name__ == '__main__':
    start_time = time.time()  # Sorgu başlangıç zamanı
    dns_server = '1.1.1.1'
    domain_to_query = 'altuga.dev'
    response = send_dns_query(dns_server, domain_to_query)
    parse_dns_response(response)
    end_time = time.time()  # Sorgu bitiş zamanı
    elapsed_time = (end_time - start_time) * 1000  # Milisaniye cinsinden hesapla
    print(f"Sorgu süresi: {elapsed_time:.2f} ms\n")
