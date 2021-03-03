# Fix Collectors Windows
- sumologic.com
- Manage Data > Collection
- Dropdown: Show: Stopped Collectors

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