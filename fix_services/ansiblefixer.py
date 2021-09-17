#!/usr/bin/env python3
# FIPS? https://www.ansible.com/blog/new-libssh-connection-plugin-for-ansible-network
# powershell commands will be encoded by default for many reasons. If you want logged turn on logging https://github.com/ansible/ansible/issues/50107#issuecomment-448442954
import argparse
from getpass import getpass
import ipaddress
import json
import os
import re
import shlex
import socket
# import subprocess
from subprocess import PIPE, run, call
import sys
import time
from types import SimpleNamespace

import requests
from requests.auth import HTTPBasicAuth


# from decouple import config
# import paramiko
# import requests
# from requests.auth import HTTPBasicAuth
# import winrm

SUMO_ACCESS_ID = os.environ.get("SUMO_ACCESS_ID")
SUMO_ACCESS_KEY = os.environ.get("SUMO_ACCESS_KEY")
SUMO_URL = os.environ.get("SUMO_URL")
print(SUMO_URL)
exit



# from remotecmd_wrapper import RcmdClient

def fix_stopped_collectors():
    # r = requests.get(SUMO_URL, auth=HTTPBasicAuth(SUMO_ACCESS_ID, SUMO_ACCESS_KEY))
    r = requests.get(SUMO_URL, auth=HTTPBasicAuth(SUMO_ACCESS_ID, SUMO_ACCESS_KEY))
    # r = requests.get("https://api.us2.sumologic.com/api/v1/collectors", auth=HTTPBasicAuth(SUMO_ACCESS_ID, SUMO_ACCESS_KEY))
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
            # time.sleep(3)
            # runcmd(host, "sumo-collector")
            # print(f"{host}: Restart service for {servicename}.")
            try:
                # print(f"host: {host} alive: {alive} os: {os}")
                print(f"Running restartSumo.ps1 on {host} where alive: {alive} os: {os}.")
                r = run(["rcmd", host, "restartSumo.ps1"],
                        stdout=PIPE, stderr=PIPE)
                if r.stderr:
                    sys.exit(r.stderr.decode())
            except Exception as e:
                # return
                print(e) 
        elif "linux" in os.lower():
            pass
            # print(f"{host} operating system is Linux.")
        else:
            pass
            # print(f"{host} appears to be down or os is not unsupported.")
            continue


def runcmdd(host, servicename):
    print(f"{host}: Restart service for {servicename}.")
    try:
        r = run(["rcmd", host, "restartSumo.ps1"],
                stdout=PIPE, stderr=PIPE)
        if r.stderr:
            sys.exit(r.stderr.decode())
    except:
        return


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


def main():
    parser = argparse.ArgumentParser(description='Run systems service checker.')
    parser.add_argument('-i', '--interval-in-seconds', required=True, type=int,
                        help='Interval in seconds to check sumologic.com and try and restart service with "Stopped Collector" status.')
    args = parser.parse_args()
    while True:
        fix_stopped_collectors()
        print(f"Waiting {args.interval_in_seconds} seconds for next loop.")
        time.sleep(args.interval_in_seconds)


if __name__ == "__main__":
    main()
