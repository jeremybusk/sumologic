# Linux
```
cd /tmp/
wget "https://collectors.sumologic.com/rest/download/linux/64" -O SumoCollector.sh && chmod +x
sudo ./SumoCollector.sh -q -Vsumo.token_and_url=<url install token> -Vsources=/opt/sumo
sudo systemctl status collector
```
This will uninstall/update old collector and Vsources must be in .json format and enabled on cloud item

# Windows
- https://help.sumologic.com/03Send-Data/Installed-Collectors/03Install-a-Collector-on-Windows
```
SumoCollector.exe -console -q "-Vsumo.token_and_url=<installationToken>" "-Vsources=<filepath>"
```


# Fix Collectors Windows
- sumologic.com
- Manage Data > Collection
- Dropdown: Show: Stopped Collectors

# Fix restart Using sc.exe
```
$sumohost = "myhostname"; Invoke-Command -ComputerName $sumohost -ScriptBlock { sc.exe failure sumo-collector reset=3600 actions= restart/30000 }
```

Login to host and as admin run something like 
```
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]'Tls12'
Invoke-WebRequest https://raw.githubusercontent.com/jeremybusk/sumologic/master/windows-install-sumologic.ps1 -outfile 'C:\temp\windows-install-sumologic.ps1'

$sumotoken = "mysecret"
$sumouninstall
powershell -c C:\temp\windows-install-sumologic.ps1 
# C:\"Program Files\Sumo Logic Collector\uninstall.exe" -q -console
# powershell -c  X:\src\sumologic\windows-install-sumologic.ps1
```

Tail main log file 
```
Get-Content -Path 'C:\Program Files\Sumo Logic Collector\logs\collector.log' -wait
```

```
Invoke-Command -ComputerName remotehost -ScriptBlock {start-service sumo-collector}
Invoke-Command -ComputerName remotehost -ScriptBlock {get-service sumo-collector | fl *}
```

Uninstall reinstall with saltstate named sumocollector
```
Invoke-Command -ComputerName remotehost -ScriptBlock {C:\"Program Files\Sumo Logic Collector\uninstall.exe" -q console; salt-call state.apply sumocollector}
```

console install
```
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]'Tls12'
Invoke-WebRequest 'https://collectors.us2.sumologic.com/rest/download/win64' -outfile 'C:\Windows\Temp\SumoCollector.exe'
C:\Windows\Temp\SumoCollector.exe -console -q "-Vclobber=false" "-Vsumo.token_and_url=<mytokenurl>" "-Vcollector.name=<myhostname>_events"
```

# Quick install
```
$install_dir="C:\tmp\sumo"
$hostname=((hostname).tolower())
$token="YOURTOKEN"
mkdir $install_dir
Invoke-WebRequest 'https://collectors.us2.sumologic.com/rest/download/win64' -outfile 'C:\Windows\Temp\SumoCollector.exe'
Invoke-WebRequest 'https://raw.githubusercontent.com/jeremybusk/sumologic/master/windows_default_sources.json' -outfile "$install_dir\sources.json"
C:\Windows\Temp\SumoCollector.exe -console -q -Vclobber=True "-Vsumo.token_and_url=$token" "-Vcollector.name=${hostname}_events" "-Vsources=$install_dir\"
```

Linux RPM example
```
/opt/SumoCollector/config/user.properties
name = <collectorName>
url=https://collectors.sumologic.com
token=SUMOXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```
systemctl restart collector

Very chatty escalation
```
function escalate_to_admin(){
  if (-Not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    if ([int](Get-CimInstance -Class Win32_OperatingSystem | Select-Object -ExpandProperty BuildNumber) -ge 6000) {
      $CommandLine = "-File `"" + $MyInvocation.MyCommand.Path + "`" " + $MyInvocation.UnboundArguments
      Start-Process -FilePath PowerShell.exe -Verb Runas -ArgumentList $CommandLine
      Exit
    }
  }
}
```
