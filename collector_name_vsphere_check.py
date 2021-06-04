from difflib import SequenceMatcher
import requests
from requests.auth import HTTPBasicAuth
import urllib3

from vmware.vapi.vsphere.client import create_vsphere_client
from sumologic import SumoLogic

vc_username = ""
vc_userpass = ""
sumo_access_id = ""
sumo_access_key = ""

vsphere2_host = ''
vsphere1_host = ''


def get_sumo_collectors():
    sumo = SumoLogic(sumo_access_id, sumo_access_key)
    sumo_collectors = sumo.collectors()
    return sumo_collectors


def get_vsphere_vms(vc_host):
    session = requests.session()
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    vsphere_client = create_vsphere_client(server=vc_host, username=vc_username, password=vc_userpass, session=session)
    vms = vsphere_client.vcenter.VM.list()
    return vms


def get_vm_name_in_sumo_collector_name_ratio(sumo_collector, vms):
    match_ratio = 0
    for vm in vms:
        vm_name = vm.name.lower()
        sumo_name = sumo_collector['name'].lower().replace('_events', '')
        new_match_ratio = SequenceMatcher(a=vm_name,b=sumo_name).ratio()
        if new_match_ratio > match_ratio:
            match_vm = vm
            match_ratio = new_match_ratio
    return match_ratio, match_vm


def main():
    vsphere1_vms = get_vsphere_vms(vsphere1_host)
    vsphere2_vms = get_vsphere_vms(vsphere2_host)
    vms =  vsphere1_vms + vsphere2_vms
    sumo_collectors = get_sumo_collectors()
    for sumo_collector in sumo_collectors:
        if sumo_collector['alive'] == False:
            match_ratio, match_vm = get_vm_name_in_sumo_collector_name_ratio(sumo_collector, vms)
            print(f"Collector: {sumo_collector['name']} VM: {match_vm.name} POWER: {match_vm.power_state} MatchRatio: {match_ratio}")


if __name__ == "__main__":
    main()
