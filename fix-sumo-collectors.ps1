# Detects and repairs broken sumologic collector services.
# Invoke-Command -ComputerName wjp1-itsupport -f ./fix-sumo.ps1 -ArgumentList 'YOU TOKEN', 0, 0
Param(
  [Parameter(Mandatory=$true)][string]$token,
  [Parameter(Mandatory=$false)][int]$forceuninstall,  # uninstall
  [Parameter(Mandatory=$false)][int]$forceinstall  # installs even when not previously installed
)
# $ErrorActionPreference = "Stop"
$ErrorActionPreference = "Continue"
$hostname=((hostname).tolower())
$install_dir="C:\Sum"


function sumo_install() {
  if(!(test-path $install_dir)){
    New-Item -ItemType Directory -Force -Path $install_dir
  }

  [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]'Tls12'
  Invoke-WebRequest 'https://collectors.us2.sumologic.com/rest/download/win64' -outfile 'C:\Windows\Temp\SumoCollector.exe'
  Invoke-WebRequest 'https://raw.githubusercontent.com/jeremybusk/sumologic/master/windows_default_sources.json' -outfile "$install_dir/sources.json"
  # -Vsources=\Windows\Temp\sources.json"

  $cmd = "C:\Windows\Temp\SumoCollector.exe"
  $args = "-console -q -Vclobber=True -Vsumo.token_and_url=${token} -Vcollector.name=${hostname}_events -Vsources=$install_dir/sources.json"
  $p = Start-Process $cmd $args -Wait -Passthru
  $p.WaitForExit()
  if ($p.ExitCode -ne 0) {
    throw "failed"
  }
}


function sumo_uninstall() {
  # C:\Program` Files\Sumo` Logic` Collector\uninstall.exe -q -console
  # C:\"Program Files\Sumo Logic Collector\uninstall.exe" -q console

  $cmd = "C:\Program Files\Sumo Logic Collector\uninstall.exe"
  $args = "-q -console"
  $p = Start-Process $cmd $args -Wait -Passthru
  $p.WaitForExit()
  if ($p.ExitCode -ne 0) {
    throw "failed"
  }
}


function sumo_start_service() {
  Write-Host "Starting sumo-collector service"
    start-service sumo-collector
}


# Tests
function test_sumo_service_exists() {
    Get-Service -name sumo-collector -ErrorAction SilentlyContinue
}


function test_sumo_service_stopped() {
  # $service = Get-Service -name sumo-collector -ErrorAction SilentlyContinue
  if ((get-service -name sumo-collector -ErrorAction SilentlyContinue ).status -eq "Stopped") {
    return $true
  }
  return $false
}


function test_sumo_service_running() {
  if ((get-service -name sumo-collector -ErrorAction SilentlyContinue ).status -eq "Running") {
    return $true
  }
  return $false
}


function get_sumo_service() {
  $service = Get-Service -name sumo-collector -ErrorAction SilentlyContinue
  if ($service.Length -gt 0) {
    if ((get-service -name sumo-collector).status -eq "Stopped") {
      write-host "ERROR: Serivce is stopped."
      # exit 1
      return "stopped"
    }
  } else {
      write-host "ERROR: Serivce does not exist on host."
      # exit 1
      return "notexist"
  }
}


function test_not_running(){
  # if ((get-service -name sumo-collector).status -eq "Running") {
  if (get-service | findstr -i sumo-collector) {
    write-host "ERROR: Serivce is already installed and running. Exiting install."
    exit 1
  }
}


function main() {
  # if ($forceinstall.IsPresent) {
  # if ($forceuninstall -eq "true") {
  if ($forceuninstall -eq $true) {
    sumo_uninstall
    Write-host "Uninstalling"
    sleep 10
    return
  }
  # if ($forceinstall -eq "true") {
  if ($forceinstall -eq $true) {
    sumo_install
    return
  }
  if (-not (test_sumo_service_exists)) {
    write-host "Service does not exist"
  } elseif (test_sumo_service_stopped) {
    write-host "Service stopped"
      sumo_start_service
      sleep 10
      if ( -not (test_sumo_service_running) ) {
        Write-host "needs to be uninstalled"
        sumo_uninstall
        write-host "uninstall complete"
        sleep 10
        write-host "installing now"
        sumo_install
      }
  } else {
    Write-Host "Host not supported."
  }
  Write-Host "Completed!"
  return
}

main
