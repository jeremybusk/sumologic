#!/usr/bin/env python3
# FIPS? https://www.ansible.com/blog/new-libssh-connection-plugin-for-ansible-network
# powershell commands will be encoded by default for many reasons. If you want logged turn on logging https://github.com/ansible/ansible/issues/50107#issuecomment-448442954
import json
import ipaddress
import re
import shlex
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
# from ssh_wrapper import SshClient
# from winrm_wrapper import WinrmClient
# from remotecmd_wrapper import WinrmClient
# from remotecmd_wrapper import SshClient
from remotecmd_wrapper import RcmdClient


# from icmplib import ping
from decouple import config
# import nmap
# from pythonping import ping
import paramiko
# from pylibsshext.errors import LibsshSessionException
# from pylibsshext.session import Session
import requests
from requests.auth import HTTPBasicAuth
import winrm


def fix_stopped_collectors(url):
    USERNAME = config('SUMO_USERNAME')
    USERPASS = config('SUMO_USERPASS')
    r = requests.get(url, auth=HTTPBasicAuth(USERNAME, USERPASS))
    if r.status_code != 200:
        print("ERROR: API 401: Invalid auth? Check user/pass or api token combo.")
        return 1
    items = r.json()['collectors']
    for item in items:
        host = item['name'].replace('_events', '')
        alive = item['alive']
        # print(f"{host} alive = {alive}")

        ### Test Values ###
        # time.sleep(5)
        # host = "testhost"
        # alive = True
        # item["osName"] = "Windows 2008"
        # print(host)


        if test_is_valid_host_or_ipaddr(host) != 0:
            continue
        if(alive != False):
            continue
        try:
            os = ""
            os = item['osName']
        except:
            continue
        if "windows" in os.lower():
            print(f"host: {host} alive: {alive} os: {os}")
            time.sleep(3)
            restart_service_if_stopped(host, "sumo-collector")
        elif "linux" in os.lower():
            print(f"{host} operating system is Linux.")
        else:
            print(f"{host} appears to be down or os is not unsupported.")
            continue


def test_os_detect(host):
    if test_tcp_port_open(host, 5986) == 0:
        return "windows"
    if test_tcp_port_open(host, 5985) == 0:
        return "windows"
    elif test_tcp_port_open(host, 22) == 0:
        return "linux"
    else:
        return "unknown"


def restart_service_if_stopped(host, servicename):
    USERNAME = config('WINRM_USERNAME')
    USERPASS = config('WINRM_USERPASS')
    print(f"{host}: Checking service status for {servicename}.")
    cmd = f"(get-service {servicename}).Status"
    # r = rcmd(host, cmd)
    # s = RcmdClient(host, USERNAME, USERPASS, transport='ntlm')
    try:
        s = RcmdClient(host, USERNAME, USERPASS, transport='ntlm', server_cert_validation='ignore')
        rsp = s.execute(cmd)
        status = rsp['out'].strip()
        print(status)
    except:
        return
    # s = RcmdClient(host, USERNAME, USERPASS, transport='ntlm')
    # s = RcmdClient(host, USERNAME, USERPASS)
    # print(r.stdout)
    # print(rsp['out'])
    # print('foo')
    # sys.exit()
    if status == "Stopped":
        print('================================')
        print(f"{host} starting stopped service {servicename}")
        print('================================')
        # time.sleep(5)
        # host = "txd1-enrapply"
        # cmd = "get-eventlog system -n 3 | Select-Object -Property * | findstr -i sumologic"
        cmd = f"start-service {servicename}"
        try:
          s = RcmdClient(host, USERNAME, USERPASS, transport='ntlm', server_cert_validation='ignore')
        except:
            return
        # s = RcmdClient(host, USERNAME, USERPASS, transport='ntlm')
        rsp = s.execute(cmd)
        # print(rsp)
        # print(r.stdout)
        # print(r.stderr)
    elif rsp == "Running":
        pass
        # print(f"{host} service status: running")
    else:
        # pass
        print(f"{host} service status: unavailble")


def test_is_valid_host_or_ipaddr(host):
    try:
        ipaddress.ip_address(host)
        return 0
    except:
        pass
    try:
        socket.gethostbyname(host)
        return 0
    except:
        print(f"E: {host} is not valid ip address or can't be resolved!")
        return 1


def test_tcp_port_open(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    address = (host, port)
    try:
        # r = s.connect_ex(address)
        s.connect((host, int(port)))
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        return 0
    # sock.settimeout(None)
    except:
        return 1


def test_ssh_rcmd():
    USERNAME = config('SSH_USERNAME')
    USERPASS = config('SSH_USERPASS')
    # s = RcmdClient('lxd0-sandbox', USERNAME, USERPASS)
    s = RcmdClient('lxd0-sandbox', USERNAME, USERPASS, rshport=22)
    rsp = s.execute("ip address")
    print(rsp['cmd'])
    print(rsp['out'])
    print(rsp['err'])
    print(rsp['exit_status'])


def test_winrm_rcmd():
    USERNAME = config('WINRM_USERNAME')
    USERPASS = config('WINRM_USERPASS')
    s = RcmdClient('ws-w10', USERNAME, USERPASS, transport='ssl', server_cert_validation='ignore')
    rsp = s.execute("ipconfig /all")
    print(rsp['cmd'])
    print(rsp['out'])
    print(rsp['err'])
    print(rsp['exit_status'])


def test_win_service_restart():
    USERNAME = config('WINRM_USERNAME')
    USERPASS = config('WINRM_USERPASS')
    restart_service_if_stopped('ws-w10', 'svsvc')


if __name__ == "__main__":
    # USERNAME = config('REMOTESHELL_USERNAME')
    # USERPASS = config('REMOTESHELL_USERPASS')
    # test_ssh_rcmd()
    # test_winrm_rcmd()
    # test_win_service_restart()
    fix_stopped_collectors("https://api.us2.sumologic.com/api/v1/collectors")
