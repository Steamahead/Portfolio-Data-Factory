# Register inflation_basket scheduled task - Mon/Wed/Fri 22:00
# Run as administrator: PowerShell -ExecutionPolicy Bypass -File setup_inflation_scheduler.ps1

$TaskName = "Portfolio Data Factory - Inflation Basket"
$XmlPath  = Join-Path $PSScriptRoot "scheduler_inflation_task.xml"
$User     = "Full STEAM Ahead"

if (-not (Test-Path $XmlPath)) {
    Write-Error "Missing $XmlPath"
    exit 1
}

# Remove existing if present (idempotent)
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Existing task found - unregistering..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$xml = Get-Content $XmlPath -Raw
Register-ScheduledTask -TaskName $TaskName -Xml $xml -User $User

Write-Host ""
Write-Host "Registered: $TaskName"
Write-Host "  Trigger: Monday/Wednesday/Friday 22:00"
Write-Host "  Action:  run_inflation_scrape.bat"
Write-Host ""
Write-Host "Verify:        Get-ScheduledTask -TaskName `"$TaskName`" | Select State, NextRunTime"
Write-Host "Run on demand: Start-ScheduledTask -TaskName `"$TaskName`""
