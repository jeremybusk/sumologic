$cmd = "C:\Windows\Temp\SumoCollector.exe"
$args = "-console -q -Vclobber=True -Vsumo.token_and_url=<your token> -Vcollector.name=wjp1-itsupport_events -Vsources=\Windows\Temp\sources.json"
$p = Start-Process $cmd $args -Wait -Passthru
$p.WaitForExit()
if ($p.ExitCode -ne 0) {
    throw "failed"
}
