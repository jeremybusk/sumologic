 Invoke-Command -ComputerName myhost -ScriptBlock {start-service sumo-collector}
  Invoke-Command -ComputerName myhost -ScriptBlock {get-service sumo-collector}
