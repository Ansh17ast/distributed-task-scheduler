# scripts/chaos/leader-kill.ps1
# Deterministic Leader Termination and Transparent Failover Experiment

Write-Host "================================================================================" -ForegroundColor Yellow
Write-Host " START: Experiment EXP-COORD-KILL (Leader Kill & Replacement)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "FAULT INJECTED: Terminate active cluster leader process"
Write-Host "OBSERVATION: Remaining quorum (2/3) detects leader heartbeat loss and triggers election"
Write-Host "RECOVERY: New leader elected; uncommitted writes prevented; committed state preserved"
Write-Host "INVARIANT CHECK: INV-CHAOS-01 (Single Leader per Term) & INV-CHAOS-02 (Zero State Loss)"

go test -v -run "TestChaos_LeaderKillAndReplacement" ./internal/chaos
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`nPASS: Leader failover completed without state loss." -ForegroundColor Green
} else {
    Write-Host "`nFAIL: Leader kill experiment violated invariant." -ForegroundColor Red
}

exit $exitCode
