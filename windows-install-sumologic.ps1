# Install and register sumologic collector via token with specified default sources.json
# https://help.sumologic.com/03Send-Data/Installed-Collectors/05Reference-Information-for-Collector-Installation/04Add_a_Collector_to_a_Windows_Machine_Image
# Usage: ./windows-install-sumologic.ps1 -t <my secret token>
# Uninstall: C:\Program Files\Sumo Logic Collector\uninstall.exe -q -console


Param(
  [Parameter(Mandatory=$true)]
  [string]$token,
  [string]$hostname = ($env:computerName).tolower()
)
$ErrorActionPreference = "Stop"
$install_dir="C:\Sum"


function install(){  
  if(!(test-path $install_dir)){
    New-Item -ItemType Directory -Force -Path $install_dir
  }

  [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]'Tls12'
  Invoke-WebRequest 'https://collectors.us2.sumologic.com/rest/download/win64' -outfile 'C:\Windows\Temp\SumoCollector.exe'
  Invoke-WebRequest 'https://raw.githubusercontent.com/jeremybusk/sumologic/master/windows_default_sources.json' -outfile "$install_dir\sources.json"
  C:\Windows\Temp\SumoCollector.exe -console -q -Vclobber=True "-Vsumo.token_and_url=$token" "-Vcollector.name=$hostname_events" "-Vsources=$install_dir\"
}


# Tests
function test_sumo_collector_service_not_running(){
  if ((get-service -name sumo-collector).status -ne "Running"){
    write-host "ERROR: Serivce is not running. Install appears to have failed."
    exit 1
  }
}


function test_not_running(){
  if ((get-service -name sumo-collector).status -eq "Running"){
    write-host "ERROR: Serivce is already installed and running. Exiting install."
    exit 1
  }
  
function main(){
  test_not_running
  install
}

main
