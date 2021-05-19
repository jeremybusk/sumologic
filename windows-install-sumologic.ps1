# Install and register sumologic collector via token with specified default sources.json
# https://help.sumologic.com/03Send-Data/Installed-Collectors/05Reference-Information-for-Collector-Installation/04Add_a_Collector_to_a_Windows_Machine_Image
# Usage: ./windows-install-sumologic.ps1 -t <my secret token>
# Uninstall: C:\Program Files\Sumo Logic Collector\uninstall.exe -q -console
# C:\"Program Files\Sumo Logic Collector\uninstall.exe" -q -console

if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  write-host "Must be ran as admin."
  exit 1
}
if(!$sumotoken){
  $sumotoken = Read-Host "Enter token"
}
$ErrorActionPreference = "Stop"
$install_dir="C:\Sum"
$hostname=((hostname).tolower())
$hostname = $hostname + "_events"
write-host "$hostname sumo install beginning ..."


function install() {
  if(!(test-path $install_dir)){
    New-Item -ItemType Directory -Force -Path $install_dir
  }

  [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]'Tls12'
  Invoke-WebRequest 'https://collectors.us2.sumologic.com/rest/download/win64' -outfile 'C:\Windows\Temp\SumoCollector.exe'
  Invoke-WebRequest 'https://raw.githubusercontent.com/jeremybusk/sumologic/master/windows_default_sources.json' -outfile "$install_dir\sources.json"
  C:\Windows\Temp\SumoCollector.exe -console -q -Vclobber=True "-Vsumo.token_and_url=$sumotoken" "-Vcollector.name=$hostname" "-Vsources=$install_dir\"
}


function test_sumo_collector_service_not_running() {
  if ((get-service -name sumo-collector).status -ne "Running") {
    write-host "ERROR: Serivce is not running. Install appears to have failed."
    exit 1
  }
}


function test_not_running(){
  # if ((get-service -name sumo-collector).status -eq "Running") {
  if (get-service | findstr -i sumo-collector) {
    write-host "ERROR: Serivce is already installed and running. Exiting install."
    exit 1
  }
}


function sumouninstall(){
  C:\Program` Files\Sumo` Logic` Collector\uninstall.exe -q -console
  write-host "Waiting 30 seconds to make sure sumo has been uninstalled."  # Fix with while checker??
  sleep 30  # I know this is bad taste but sumo sucketh
}


function main() {
  if($sumouninstall){
    sumouninstall
  }
  test_not_running
  install
}

main
