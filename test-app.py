from datetime import datetime
import socket
import time
from urllib.parse import urlparse

import dns.resolver

logfile = "app.log" 


def get_tcp_port_rsp_time(host, port):
    start = time.time()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((str(host), int(port)))
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        tcp_rsp_time = time.time() - start
    except Exception as e:
        tcp_rsp_time = None
        str(e)
    return tcp_rsp_time


def get_dnstime(fqdn):
    dns_start = time.time()
    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = ['8.8.8.8', '8.8.4.4', '1.1.1.1']
    try:
        dns_response = resolver.resolve(fqdn, 'A')
        dns_time = time.time() - dns_start
    except:
        dns_time = None 
    return dns_time


while True:
    now = datetime.now()
    ts = now.strftime("%Y-%m-%d:%H-%M-%S")
    url = "https://example.com"
    fqdn = urlparse(url).hostname
    msg = {}
    msg['dns_time'] = get_dnstime(fqdn)
    msg['host'] = fqdn 
    msg['port'] = 443
    rsp_time = get_tcp_port_rsp_time(msg['host'], msg['port'])
    msg['rsp_time'] = rsp_time 
    msg['tag'] = "feefiifoo" 
    msg['ts'] = ts 
    print(msg)
    with open(logfile, 'a') as f:
        f.write(f"{msg}\n")
    time.sleep(10)
