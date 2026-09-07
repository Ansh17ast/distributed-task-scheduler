# scripts/chaos/zombie-worker.ps1
# Deterministic Zombie Worker Fencing & Reassignment Experiment

Write-Host "================================================================================" -ForegroundColor Yellow
Write-Host " START: Experiment EXP-ZOMBIE-FENCING (Zombie Worker Fencing & Lease Epochs)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "FAULT INJECTED: Worker A (Epoch 1) isolated; lease expires; Worker B (Epoch 2) executes task"
Write-Host "OBSERVATION: Worker A attempts delayed completion with stale Epoch 1"
Write-Host "RECOVERY: Coordinator fences Worker A; rejects stale report; preserves Worker B state"
Write-Host "INVARIANT CHECK: INV-CHAOS-04 (Zombie Fencing) & INV-CHAOS-06 (No Orphaned Tasks)"

go test -v -run "TestChaos_ZombieWorkerFencing" ./internal/chaos
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`nPASS: Zombie worker fenced; authoritative state protected." -ForegroundColor Green
} else {
    Write-Host "`nFAIL: Zombie worker fencing experiment failed." -ForegroundColor Red
}

exit $exitCode
