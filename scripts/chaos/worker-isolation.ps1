# scripts/chaos/worker-isolation.ps1
# Deterministic Worker Disconnect and Reconnection Experiment

Write-Host "================================================================================" -ForegroundColor Yellow
Write-Host " START: Experiment EXP-WORKER-RECONNECT (Worker Network Disconnect & Reconnect)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "FAULT INJECTED: Worker network connection paused during task execution"
Write-Host "OBSERVATION: Worker heartbeats resume within lease validity window"
Write-Host "RECOVERY: Coordinator accepts valid session heartbeat and allows task completion"
Write-Host "INVARIANT CHECK: INV-CHAOS-07 (Slot Capacity Accounting)"

go test -v -run "TestChaos_WorkerReconnectBeforeLeaseExpiry" ./internal/chaos
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`nPASS: Worker reconnection verified successfully." -ForegroundColor Green
} else {
    Write-Host "`nFAIL: Worker reconnection experiment failed." -ForegroundColor Red
}

exit $exitCode
