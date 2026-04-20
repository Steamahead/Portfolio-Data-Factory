# ============================================================
#  Portfolio Data Factory - Setup Task Scheduler
#  Uruchom jako Administrator:
#    powershell -ExecutionPolicy Bypass -File setup_task_scheduler.ps1
# ============================================================

$ProjectDir = "C:\Users\sadza\PycharmProjects\portfolio-data-factory"

# ── 1. ETL Daily: Energy Prophet + Gov Spending (08:00) ──────
$action1 = New-ScheduledTaskAction `
    -Execute "$ProjectDir\run_etl_daily.bat" `
    -WorkingDirectory $ProjectDir

$trigger1 = New-ScheduledTaskTrigger -Daily -At "08:00"

$settings1 = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask `
    -TaskName "PortfolioDataFactory\ETL_Daily" `
    -Action $action1 `
    -Trigger $trigger1 `
    -Settings $settings1 `
    -Description "Energy Prophet + Gov Spending Radar (CSV-Only mode)" `
    -Force

Write-Host "[OK] ETL_Daily registered (08:00 daily)" -ForegroundColor Green

# ── 2. CEE FX Hourly ────────────────────────────────────────
$action2 = New-ScheduledTaskAction `
    -Execute "$ProjectDir\run_cee_fx_hourly.bat" `
    -WorkingDirectory $ProjectDir

# Hourly trigger: repeat every 1 hour for 24 hours, daily
$trigger2 = New-ScheduledTaskTrigger -Daily -At "00:00"
$trigger2.Repetition = (New-ScheduledTaskTrigger -Once -At "00:00" `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Hours 24)).Repetition

$settings2 = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

Register-ScheduledTask `
    -TaskName "PortfolioDataFactory\CEE_FX_Hourly" `
    -Action $action2 `
    -Trigger $trigger2 `
    -Settings $settings2 `
    -Description "CEE FX Volatility - hourly (CSV-Only mode)" `
    -Force

Write-Host "[OK] CEE_FX_Hourly registered (every 1h)" -ForegroundColor Green

# ── 3. Verify ──────────────────────────────────────────────
Write-Host "`n=== Registered Tasks ===" -ForegroundColor Cyan
Get-ScheduledTask -TaskPath "\PortfolioDataFactory\*" | Format-Table TaskName, State, @{L="NextRun";E={(Get-ScheduledTaskInfo $_).NextRunTime}}
