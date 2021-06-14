#!/usr/bin/env python3
from difflib import SequenceMatcher
import requests
from requests.auth import HTTPBasicAuth
import urllib3

from decouple import config
from vmware.vapi.vsphere.client import create_vsphere_client
from sumologic import SumoLogic
import sys


# Vsphere
vc_username = config('VC_USERNAME') 
vc_userpass = config('VC_USERPASS') 
vsphere1_host = config('VSPHERE1_HOST') 
vsphere2_host = config('VSPHERE2_HOST') 
# Sumo
sumo_access_id = config('SUMO_ACCESS_ID') 
sumo_access_key = config('SUMO_ACCESS_KEY') 


sumo = SumoLogic(sumo_access_id, sumo_access_key)


def delete_collector(sumo_collector):
    url = f"https://api.us2.sumologic.com/api/v1/collectors/{sumo_collector['id']}"
    r = requests.delete(url, auth=(sumo_access_id, sumo_access_key))
    print(f"I: Deleting {sumo_collector['id']} {sumo_collector['name']} rsp: {r}")
    return r


def delete_powered_off_vm_sumo_collectors(sumo_collectors, vms):
    for sumo_collector in sumo_collectors:
        if sumo_collector['alive'] == False:
            match_ratio, match_vm = get_vm_name_in_sumo_collector_name_ratio(sumo_collector, vms)
            print(f"Collector: {sumo_collector['name']} VM: {match_vm.name} POWER: {match_vm.power_state} MatchRatio: {match_ratio}")
            if match_ratio > .9 and match_vm.power_state == "POWERED_OFF":
                delete_collector(sumo_collector)


def get_sumo_collectors():
    sumo_collectors = sumo.collectors()
    return sumo_collectors


def get_vm_name_in_sumo_collector_name_ratio(sumo_collector, vms):
    match_ratio = 0
    for vm in vms:
        vm_name = vm.name.lower()
        sumo_name = sumo_collector['name'].lower().replace('_events', '')
        if sumo_name in vm_name:
            new_match_ratio = .95
        else:
            new_match_ratio = SequenceMatcher(a=vm_name,b=sumo_name).ratio()
        if new_match_ratio > match_ratio:
            match_vm = vm
            match_ratio = new_match_ratio
    return match_ratio, match_vm


def get_vsphere_vms(vc_host):
    session = requests.session()
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    vsphere_client = create_vsphere_client(server=vc_host, username=vc_username, password=vc_userpass, session=session)
    vms = vsphere_client.vcenter.VM.list()
    return vms



def main():
    vsphere1_vms = get_vsphere_vms(vsphere1_host)
    vsphere2_vms = get_vsphere_vms(vsphere2_host)
    vms =  vsphere1_vms + vsphere2_vms
    sumo_collectors = get_sumo_collectors()
    delete_powered_off_vm_sumo_collectors(sumo_collectors, vms)


if __name__ == "__main__":
    main()
