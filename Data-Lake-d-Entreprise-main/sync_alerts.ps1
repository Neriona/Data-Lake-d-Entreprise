# Continuous alert sync from Docker to host
param(
    [int]$IntervalSeconds = 5
)

Write-Host "Alert Sync Service Started - Syncing every $IntervalSeconds seconds"
Write-Host "Press Ctrl+C to stop"

while ($true) {
    try {
        docker cp airflow-webserver:/opt/airflow/alerts/. alerts/ 2>&1 | Out-Null
        docker cp airflow-scheduler:/opt/airflow/alerts/. alerts/ 2>&1 | Out-Null
        
        $alertCount = @(Get-ChildItem "alerts" -Force -Include "*.json" -ErrorAction SilentlyContinue).Count
        if ($alertCount -gt 0) {
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] ✓ Synced - $alertCount alert files on host"
        }
    } catch {
        # Silently continue
    }
    
    Start-Sleep -Seconds $IntervalSeconds
}
