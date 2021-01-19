 ### Install on windows with default sources from event logs
 ```
 [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]'Tls12'; Invoke-WebRequest 'https://collectors.us2.sumologic.com/rest/download/win64' -outfile 'C:\Windows\Temp\SumoCollector.exe'; Invoke-WebRequest 'https://raw.githubusercontent.com/jeremybusk/sumologic/master/windows_default_sources.json' -outfile "\tmp\sources.json"; C:\Windows\Temp\SumoCollector.exe -console -q "-Vclobber=True -Vsumo.token_and_url=<token>" "-Vcollector.name=$(hostname)_events" "-Vsources=\tmp\sources.json"
 ```

 ### Uninstall on Windows
```
C:\Program` Files\Sumo` Logic` Collector\uninstall.exe -q -console
```
