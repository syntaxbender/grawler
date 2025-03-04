import dns.resolver
import socket
from urllib.parse import urlparse
import json
import queue
import threading
class Grawler:
  def __init__(self, urls_file_path, dns_server="1.1.1.1", dns_timeout=0.3,syn_timeout=0.3,chock=1000,thread_count=100):
      print("Grawler initializing..")
      self.dns_threads = []
      self.tcp_threads = []
      self.dns_server = dns_server
      self.dns_timeout = dns_timeout
      self.syn_timeout = syn_timeout
      self.urls_file_path=urls_file_path
      self.chock = chock
      self.thread_count = thread_count
      self.urls_queue = queue.Queue()
      self.urls_dict = {}
      self.domains_ips = {}
      self.domains_set = set()
      self.domains_queue = queue.Queue()
      self.domains_dns_valid_queue = queue.Queue()
      self.domains_dns_nonvalid_queue = queue.Queue()
      self.urls_tcp_valid_queue = queue.Queue()
      self.urls_tcp_nonvalid_queue = queue.Queue()
      self.urls_data = self.loadJson(urls_file_path)
      
  def loadJson(self, path):
      cursor=0
      print("Importing json file...")
      file = open(path, "r")
      data = json.load(file)
      for i in range(len(data)):
        if cursor >= self.chock:
          break
        for i2 in range(len(data[i]["urls"])):
          if cursor >= self.chock:
            break
          try:
            domain,protokol,port = self.get_domain_and_protocol(data[i]["urls"][i2])
          except ValueError as e:
            print(f"{e}")
            continue
          url = data[i]["urls"][i2]
          url_path = [i,"urls",i2]
          
          if domain not in self.domains_set:
            self.domains_set.add(domain)
            self.domains_queue.put(domain)
            
          if url not in self.urls_dict:
            url_info={
              "url":url,
              "domain":domain,
              "port":port,
              "protokol":protokol,
              "cve":data[i]["cve_id"],
              "path":url_path
            }
            self.urls_queue.put(url_info)
          self.urls_dict.setdefault(url, []).append(url_path)
          cursor+=1
      # self.urls_dict = None
      self.domains_set = None
      file.close()
      return None
  def get_domain_and_protocol(self, url):
      parsed_url = urlparse(url)
      protocol = parsed_url.scheme
      
      if not protocol or (protocol != "http" and protocol != "https"):
        raise ValueError(f"Incorrect domain format for {url}")
      domain = parsed_url.netloc if parsed_url.netloc else parsed_url.path
      port = 80 if protocol == "http" else 443
      return domain, protocol, port

  def dns_query(self, domain):
      try:
          resolver = dns.resolver.Resolver()
          resolver.lifetime = self.dns_timeout
          resolver.nameservers = [self.dns_server]
          answers = dns.resolver.resolve(domain, 'A')
          return [answer.to_text() for answer in answers]
      except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.Timeout):
          return None
        
  def check_syn_request(self, url_info,ips):
      port = url_info["port"]
      try:
          for ip in ips:
            sock = socket.create_connection((ip, port), timeout=self.syn_timeout)
            sock.close()
            return True
      except (socket.timeout, socket.error) as e:
          print(url_info["domain"],e)

          pass
      return False
        
  def domain_dns_worker(self):
    while not self.domains_queue.empty():
      domain = self.domains_queue.get()
      print(domain)
      ips = self.dns_query(domain)
      if ips:
        self.domains_dns_valid_queue.put([domain,ips])
      else:
        self.domains_dns_nonvalid_queue.put(domain)
  def domain_queue_to_dict(self):
    while not self.domains_dns_valid_queue.empty():
      domain_info = self.domains_dns_valid_queue.get()
      self.domains_ips[domain_info[0]] = domain_info[1]
    while not self.domains_dns_nonvalid_queue.empty():
      domain_info = self.domains_dns_nonvalid_queue.get()
      self.domains_ips[domain_info[0]] = False
    self.domains_dns_valid_queue = None
    self.domains_dns_nonvalid_queue = None
      
  def urls_tcp_syn_worker(self):
    while not self.urls_queue.empty():
      url_info = self.urls_queue.get()
      check = False
      if url_info["domain"] in self.domains_ips:
        ips=self.domains_ips[url_info["domain"]]
        check = self.check_syn_request(url_info,ips)
      if check:
        self.urls_tcp_valid_queue.put(url_info)
      else:
        self.urls_tcp_nonvalid_queue.put(url_info)
  def thread_manager(self,thread_container,worker_func,thread_count):
    for _ in range(thread_count):
        t = threading.Thread(target=worker_func)
        t.start()
        thread_container.append(t)

    for t in thread_container:
        t.join()
        
  def grawl(self):
    dns_thread_count = min(self.thread_count, self.domains_queue.qsize())
    self.thread_manager(self.dns_threads,self.domain_dns_worker,dns_thread_count)
    
    self.domain_queue_to_dict()
    
    tcp_thread_count = min(self.thread_count, self.urls_queue.qsize())
    self.thread_manager(self.tcp_threads,self.urls_tcp_syn_worker,tcp_thread_count)


if __name__ == "__main__":
  crawler = Grawler("./dataset.json","1.1.1.1",0.3,0.3,10,100)
  crawler.grawl()
  print(list(crawler.urls_tcp_valid_queue.queue))
  print(list(crawler.urls_tcp_nonvalid_queue.queue))
  print(crawler.domains_ips)
