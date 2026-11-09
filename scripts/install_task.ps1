# Runs laneq on this Windows PC as a background service for every app that uses it:
# a scheduled task (current user, no admin) that starts laneq-bg.exe at login and
# restarts it if it crashes. It doesn't start any app: each app starts its own worker,
# which connects to http://127.0.0.1:7070 and shows up in the dashboard's Apps panel.
#
#   powershell -ExecutionPolicy Bypass -File scripts\install_task.ps1             # install / update, and start now
#   powershell -ExecutionPolicy Bypass -File scripts\install_task.ps1 -Uninstall  # remove it (the data stays)
#
# Data and log: %USERPROFILE%\.laneq\ (data\, laneq.log). Build laneq-bg.exe first:
#   go build -ldflags "-H=windowsgui" -o laneq-bg.exe ./cmd/laneq
# Power off (dashboard button, or POST /v1/admin/poweroff) exits cleanly, so it isn't restarted until the next login.
param([switch]$Uninstall, [string]$Addr = "127.0.0.1:7070")

$name = "laneq"
if ($Uninstall) {
    try { Invoke-RestMethod -Method Post "http://$Addr/v1/admin/shutdown" -TimeoutSec 5 | Out-Null } catch {}
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    Write-Output "Removed the '$name' task. Data is still in $env:USERPROFILE\.laneq"
    return
}

$exe  = Join-Path (Split-Path $PSScriptRoot) "laneq-bg.exe"
$home_ = Join-Path $env:USERPROFILE ".laneq"
if (-not (Test-Path $exe)) { throw "missing $exe - build it first (see the top of this script)" }
New-Item -ItemType Directory -Force $home_ | Out-Null

$args_ = "-addr $Addr -data `"$home_\data`" -log `"$home_\laneq.log`""
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName $name -Force `
    -Description "laneq job queue for every app on this PC (dashboard: http://$Addr)" `
    -Action (New-ScheduledTaskAction -Execute $exe -Argument $args_) `
    -Trigger (New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME") -Settings $settings | Out-Null

Start-ScheduledTask -TaskName $name
$deadline = (Get-Date).AddSeconds(10)
while ((Get-Date) -lt $deadline) {
    try { Invoke-RestMethod "http://$Addr/healthz" -TimeoutSec 2 | Out-Null; Write-Output "laneq is running: http://$Addr"; return } catch { Start-Sleep -Milliseconds 300 }
}
Write-Output "Installed, but laneq isn't answering yet. Check $home_\laneq.log"
