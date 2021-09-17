#!/usr/bin/env python3
# FIPS? https://www.ansible.com/blog/new-libssh-connection-plugin-for-ansible-network
# powershell commands will be encoded by default for many reasons. If you want logged turn on logging https://github.com/ansible/ansible/issues/50107#issuecomment-448442954
import argparse
from getpass import getpass
import ipaddress
import json
import re
import shlex
import socket
import subprocess
import sys
import time
from types import SimpleNamespace


from decouple import config
import paramiko
import requests
from requests.auth import HTTPBasicAuth
import winrm


from remotecmd_wrapper import RcmdClient


def fix_stopped_collectors(url, sumo_access_id, sumo_access_key, username, userpass):
    r = requests.get(url, auth=HTTPBasicAuth(sumo_access_id, sumo_access_key))
    if r.status_code != 200:
        print("ERROR: API 401: Invalid auth? Check user/pass or api token combo.")
        return 1
    items = r.json()['collectors']
    for item in items:
        host = item['name'].replace('_events', '')
        alive = item['alive']
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
            # time.sleep(3)
            restart_service_if_stopped(host, username, userpass, "sumo-collector")
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


def restart_service_if_stopped(host, username, userpass, servicename):
    print(f"{host}: Checking service status for {servicename}.")
    cmd = f"(get-service {servicename}).Status"
    try:
        s = RcmdClient(host, username, userpass, transport='ntlm', server_cert_validation='ignore')
        rsp = s.execute(cmd)
        status = rsp['out'].strip()
        print(status)
    except:
        return
    if status == "Stopped":
        print('================================')
        print(f"{host} starting stopped service {servicename}")
        print('================================')
        cmd = f"start-service {servicename}"
        try:
          s = RcmdClient(host, username, userpass, transport='ntlm', server_cert_validation='ignore')
        except:
            return
        rsp = s.execute(cmd)
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


def test_ssh_rcmd(host, username, userpass,  cmd='ip address'):
    s = RcmdClient(host, username, userpass, rshport=22)
    rsp = s.execute(cmd)
    print(rsp['cmd'])
    print(rsp['out'])
    print(rsp['err'])
    print(rsp['exit_status'])


def test_winrm_rcmd(host, username, userpass, cmd='ipconfig /all'):
    s = RcmdClient(host, username, userpass, transport='ssl', server_cert_validation='ignore')
    rsp = s.execute(cmd)
    print(rsp['cmd'])
    print(rsp['out'])
    print(rsp['err'])
    print(rsp['exit_status'])


def test_win_service_restart(host, username, userpass, service='sumo-collector'):
    restart_service_if_stopped(host, username, userpass, service)


def main():
    parser = argparse.ArgumentParser(description='Run systems service checker.')
    parser.add_argument('-s', '--sumo-access-id', required=False, type=str,
                        help='Sumo access id.')
    parser.add_argument('-u', '--username', required=False, type=str,
                        help='Windows AD username')
    parser.add_argument('-i', '--interval-in-seconds', required=True, type=int,
                        help='Interval in seconds to check sumologic.com and try and restart service with "Stopped Collector" status.')
    args = parser.parse_args()
    if args.username:
        args.password = getpass(prompt='Windows AD user password: ')
    else:
        args.username = config('USERNAME')
        args.password = config('USERPASS')
    if args.sumo_access_id:
        args.sumo_access_key = getpass(prompt="Sumologic access id access key")
    else:
        args.sumo_access_id = config('SUMO_ACCESS_ID')
        args.sumo_access_key = config('SUMO_ACCESS_KEY')

    while True:
        fix_stopped_collectors("https://api.us2.sumologic.com/api/v1/collectors", args.sumo_access_id, args.sumo_access_key, args.username, args.password)
        print(f"Waiting {args.interval_in_seconds} seconds for next loop.")
        time.sleep(args.interval_in_seconds)


if __name__ == "__main__":
    main()
    # USERNAME = config('REMOTESHELL_USERNAME')
    # USERPASS = config('REMOTESHELL_USERPASS')
    # test_ssh_rcmd(myhost)
    # test_winrm_rcmd(myhost)
    # test_win_service_restart()
    # fix_stopped_collectors("https://api.us2.sumologic.com/api/v1/collectors")
