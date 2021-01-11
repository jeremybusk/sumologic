#
$hostnames = @(
    'host1'
    'host2'
)
foreach ($hostname in $hostnames) {
  Invoke-Command -ComputerName $hostname -f "C:\tmp\windows-install-sumologic.ps1"
}
