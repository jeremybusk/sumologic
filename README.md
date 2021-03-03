# Fix Collectors
- sumologic.com
- Manage Data > Collection
- Dropdown: Show: Stopped Collectors

```
Invoke-Command -ComputerName remotehost -ScriptBlock {start-service sumo-collector}
Invoke-Command -ComputerName remotehost -ScriptBlock {get-service sumo-collector}
```
