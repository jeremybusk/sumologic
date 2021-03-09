cmd = "C:\Program Files\Sumo Logic Collector\uninstall.exe"
$args = "-q -console"
# $p = Start-Process C:\"Program Files\Sumo Logic Collector\uninstall.exe" -q -console -Wait -Passthru
$p = Start-Process $cmd $args -Wait -Passthru
$p.WaitForExit()
if ($p.ExitCode -ne 0) {
    throw "failed"
}
