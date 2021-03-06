#!/usr/bin/env python3
# FIPS? https://www.ansible.com/blog/new-libssh-connection-plugin-for-ansible-network
# powershell commands will be encoded by default for many reasons. If you want logged turn on logging https://github.com/ansible/ansible/issues/50107#issuecomment-448442954
import json
import requests
import subprocess
from requests.auth import HTTPBasicAuth
import time
import re
import winrm
# from pythonping import ping
from icmplib import ping
import socket
import ipaddress
# import nmap
from decouple import config
import sys
from pylibsshext.errors import LibsshSessionException
from pylibsshext.session import Session
import shlex


def fix_stopped_collectors(url):
    USERNAME = config('SUMO_USERNAME')
    USERPASS = config('SUMO_USERPASS')
    r = requests.get(url, auth=HTTPBasicAuth(USERNAME, USERPASS))
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
        if(alive == False):
            continue
        try:
            os = ""
            os = item['osName']
        except:
            continue
        if "windows" in os.lower():
            print(f"{host} {alive} {os}")
            restart_service_if_stopped(host, "sumo-collector")
        elif "linux" in os.lower():
            print(f"{host} operating system is Linux.")
        else:
            print(f"{host} appears to be down or os is not unsupported.")
            continue


def test_os_detect(host):
    if test_tcp_port_open(host, 5985) == 0:
        return "windows"
    elif test_tcp_port_open(host, 22) == 0:
        return "linux"
    else:
        return "unknown"


def restart_service_if_stopped(host, servicename):
    print(f"{host} starting service {servicename}")
    cmd = f"(get-service {servicename}).Status"
    r = rcmd(host, cmd)
    print(r.stdout)
    status = r.stdout
    if status == "Stopped":
        # cmd = "get-eventlog system -n 3 | Select-Object -Property * | findstr -i sumologic"
        cmd = "start-service sumo-collector"
        r = rcmd(host, cmd)
        # print(r.stdout)
        # print(r.stderr)
    elif status == "Running":
        pass
        # print(f"{host} service status: running")
    else:
        pass
        # print(f"{host} service status: unavailble")


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


def rpsh(host, cmd):
    rcmd = f"invoke-command -computername {host} -ScriptBlock {{ {cmd} }}"
    r = subprocess.run(["powershell", "-Command", rcmd], capture_output=True)
    return r

def rrmdcli():
    # This can convert a windows cmd command to cli
    # Were all commands should be powershell this should be avoided
    chars = ["(", ")", "{", "}", "|"]
    runcmd = None
    if not ([char for char in chars if char in cmd]):
        cmd_parts = shlex.split(cmd, posix=False)
        runcmd = cmd_parts[0], cmd_parts[1:]
    if runcmd:
        r = s.run_cmd(*runcmd)
    else:
        r = s.run_ps(cmd)

def rcmd(host, cmd):
    if test_os_detect(host) == "windows":
        USERNAME = config('WINDOWS_USERNAME')
        USERPASS = config('WINDOWS_USERPASS')
        s = winrm.Session(host, auth=(USERNAME, USERPASS), transport='ntlm')
        r = s.run_ps(cmd)
        if r.status_code == 0:
            r.stdout = r.std_out.decode().strip()
            r.stderr = r.std_err.decode().strip()
            r.status = r.status_code
            return r
        else:
            r.stdout = r.std_out.decode().strip()
            r.stderr = r.std_err.decode().strip()
            print(f"E: {r.stderr}")
            r.status = r.status_code
            return r
    elif test_os_detect(host) == "linux":
        print(f"{host} OS is Linux and cmds not supported yet.")
    else:
        print(f"{host} OS is unsupported.")


def ssh_cmd(host, cmd):
    USERNAME = config('WINDOWS_USERNAME')
    USERPASS = config('WINDOWS_USERPASS')
    TIMEOUT = 30
    PORT = 22
    ssh = Session()

    try:
        ssh.connect(
            host=host,
            user=USERNAME,
            password=USERPASS,
            timeout=TIMEOUT,
            port=PORT,
        )
    except LibsshSessionException as ssh_exc:
        print(f'Failed to connect to {HOST}:{PORT} over SSH: {ssh_exc!s}')

    print(f'{ssh.is_connected}')

    ssh_channel = ssh.new_channel()
    cmd_resp = ssh_channel.write(b'ls')
    print(f'stdout:\n{cmd_resp.stdout}\n')
    print(f'stderr:\n{cmd_resp.stderr}\n')
    print(f'return code: {cmd_resp.returncode}\n')
    ssh_channel.close()

    chan_shell = ssh.invoke_shell()
    # chan_shell.sendall(b'ls')
    chan_shell.sendall(cmd)
    data = chan_shell.read_bulk_response(timeout=2, retry=10)
    chan_shell.close()
    print(data)

    ssh.close()


if __name__ == "__main__":
    fix_stopped_collectors("https://api.us2.sumologic.com/api/v1/collectors")
