## Modify the $ServerName vairble ONLY to the name of the server where you want the SumoLogic Collector started and it's service recovery properties udpates to best practices.
 ###################################
 $ServerName = "SERVER NAME"   #  Modify this line
 ###################################
 
 function Set-ServiceRecovery{
    [alias('Set-Recovery')]
    param
    (
        [string] [Parameter(Mandatory=$true)] $ServiceDisplayName,
        [string] [Parameter(Mandatory=$true)] $Server,
        [string] $action1 = "restart",
        [int] $time1 =  30000, # in miliseconds
        [string] $action2 = "restart",
        [int] $time2 =  30000, # in miliseconds
        [string] $actionLast = "restart",
        [int] $timeLast = 30000, # in miliseconds
        [int] $resetCounter = 60 # in seconds
    )
    $serverPath = "\\" + $server
    $services = Get-CimInstance -ClassName 'Win32_Service' -ComputerName $Server| Where-Object {$_.DisplayName -imatch $ServiceDisplayName}
    $action = $action1+"/"+$time1+"/"+$action2+"/"+$time2+"/"+$actionLast+"/"+$timeLast
    foreach ($service in $services){
        # https://technet.microsoft.com/en-us/library/cc742019.aspx
        $output = sc.exe $serverPath failure $($service.Name) reset= $resetCounter actions= $action
        #Write-Host  $output
    }
}
 
Set-ServiceRecovery -ServiceDisplayName "SumoLogic Collector" -Server $ServerName
Get-Service -Name "SumoLogic Collector"  -ComputerName $ServerName | Start-service
