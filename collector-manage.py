import json
import requests
import subprocess
from requests.auth import HTTPBasicAuth
import time
import re
from pythonping import ping
username = 'YOUR API ID'
userpass = 'YOUR API TOKEN'
# r = response from api url/powershell


def get(url):
    r = requests.get(url, auth=HTTPBasicAuth(username, userpass))
    items = r.json()['collectors']
    for item in items:
        host = item['name'].replace('_events', '')
        alive = item['alive']
        print(host)

        # print("sleep 5")
        # time.sleep(5)
        # host = "testhost1"
        # host = "texthost2"
        # alive = False
        # item["osName"] = "Windows 2008"

        if(alive == False):
            try:
                os = ""
                os = item['osName']
            except:
                pass
            if "windows" in os.lower():
                print(f"{host} {alive} {os}")
                try:
                    ping(host, count=2)
                    restart_service_if_stopped(host)
                except:
                    print(f"{host} b icmp appears down.")


def restart_service_if_stopped(host):
    cmd = f"((get-service sumo-collector).Status).Value"
    r = rpsh(host, cmd)
    print(r)
    status = r.stdout.decode().strip()
    if status == "Stopped":
        time.sleep(5)
        print("status: stopped")
        print(f"Starting sumo-collector on {host}")
        time.slee(5)
        # cmd = "get-eventlog system -n 3 | Select-Object -Property * | findstr -i sumologic"
        # cmd = "get-service sumo-collector"
        cmd = "start-service sumo-collector"
        o = rpsh(host, cmd)
        s = o.stdout.decode().strip()
        print(o)
    elif status == "Running":
        print(f"{host} service status: running")
        # pass
    else:
        # pass
        print(f"{host} service status: unavailble")


def rpsh(host, cmd):
    rcmd = f"invoke-command -computername {host} -ScriptBlock {{ {cmd} }}"
    r = subprocess.run(["powershell", "-Command", rcmd], capture_output=True)
    return r


if __name__ == "__main__":
    get("https://api.us2.sumologic.com/api/v1/collectors")
