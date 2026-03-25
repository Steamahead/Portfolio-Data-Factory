# Portfolio Data Factory - Setup Scheduled Task
# Run as Administrator!

$ErrorActionPreference = "Stop"

# 1. Usun stary duplikat
try {
    Unregister-ScheduledTask -TaskName "PortfolioDataFactory_DailyScrapers" -TaskPath "\PortfolioDataFactory\" -Confirm:$false
    Write-Host "[OK] Usunięto: PortfolioDataFactory_DailyScrapers" -ForegroundColor Green
} catch {
    Write-Host "[SKIP] PortfolioDataFactory_DailyScrapers nie istnieje lub już usunięty" -ForegroundColor Yellow
}

# 2. Usun obecny task (zastąpimy nowym)
try {
    Unregister-ScheduledTask -TaskName "Portfolio Data Factory - Daily Scrapers" -Confirm:$false
    Write-Host "[OK] Usunięto: Portfolio Data Factory - Daily Scrapers" -ForegroundColor Green
} catch {
    Write-Host "[SKIP] Portfolio Data Factory - Daily Scrapers nie istnieje" -ForegroundColor Yellow
}

# 3. Importuj nowy task z XML
$xmlPath = "C:\Users\sadza\PycharmProjects\Portfolio-Data-Factory\scheduler_task.xml"
Register-ScheduledTask -TaskName "Portfolio Data Factory - Daily Scrapers" -Xml (Get-Content $xmlPath | Out-String) -User "Full STEAM Ahead"
Write-Host "[OK] Zarejestrowano nowy task z 2 triggerami (19:00 + logon)" -ForegroundColor Green

# 4. Weryfikacja
$task = Get-ScheduledTask -TaskName "Portfolio Data Factory - Daily Scrapers"
Write-Host "`nTask: $($task.TaskName)"
Write-Host "State: $($task.State)"
Write-Host "Triggers: $($task.Triggers.Count)"
foreach ($t in $task.Triggers) {
    if ($t -is [Microsoft.Management.Infrastructure.CimInstance]) {
        $type = $t.CimClass.CimClassName
        Write-Host "  - $type (Enabled=$($t.Enabled))"
    }
}

# 5. Usun pusty folder PortfolioDataFactory z Task Scheduler (jeśli został)
try {
    $folder = (New-Object -ComObject Schedule.Service)
    $folder.Connect()
    $folder.GetFolder("\").DeleteFolder("PortfolioDataFactory", 0)
    Write-Host "[OK] Usunięto pusty folder PortfolioDataFactory" -ForegroundColor Green
} catch {
    # folder nie istnieje lub nie jest pusty - OK
}

Write-Host "`n[DONE] Scheduler skonfigurowany." -ForegroundColor Cyan
Write-Host "  Trigger 1: Codziennie 19:00"
Write-Host "  Trigger 2: At logon + 3 min delay (catch-up)"
Write-Host "  RunOnlyIfNetworkAvailable: True"
Write-Host "  StopIfGoingOnBatteries: False"
Write-Host "  StartWhenAvailable: True"

Read-Host "`nNaciśnij Enter aby zamknąć"
