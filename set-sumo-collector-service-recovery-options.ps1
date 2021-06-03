# $cred = Get-Credential  # This is to use different credential than your account
$domain = "example.com"
$search1 = "s*"
$search2 = "t*"
$search3 = "u*"

$SearchBase = ""
$domain.Split(".") | ForEach {
    $SearchBase = $SearchBase + "DC=$_,"
 }
$SearchBase = $SearchBase.Substring(0,$SearchBase.Length-1)

$hostnames = (Get-ADComputer -Filter {OperatingSystem -like "Windows*"} | where {$_.Name -like $search1 -or $_.Name -like $search2  -or $_.Name -like $search3} | select-object -expandproperty name)
foreach ($hostname in $hostnames)
{
  $fqdn = "$hostname.$domain"
  # $r = (Invoke-Command -ComputerName $fqdn -ScriptBlock { $s = "sumo-collector"; if((get-service $s -ErrorAction SilentlyContinue).Length -gt 0){sc.exe failure $s reset=3600 actions= restart/30000} } )
  $r = (Invoke-Command -ComputerName $fqdn -ScriptBlock { $s = "sumo-collector"; if(get-service $s){sc.exe failure $s reset=3600 actions= restart/30000} } 2>&1)
  Write-Host "$fqdn | $r"
}
