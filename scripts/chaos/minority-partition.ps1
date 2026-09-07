# scripts/chaos/minority-partition.ps1
# Deterministic Network Partition Experiment (Leader Isolated in Minority)

Write-Host "================================================================================" -ForegroundColor Yellow
Write-Host " START: Experiment EXP-PART-CASE3 (Leader Isolated as Minority Partition)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "FAULT INJECTED: Form network partition: {C1} vs {C2, C3}"
Write-Host "OBSERVATION: Isolated C1 loses quorum; attempted writes fail; {C2, C3} elect new leader"
Write-Host "RECOVERY: Heal partition; C1 steps down and converges to new leader log"
Write-Host "INVARIANT CHECK: INV-CHAOS-03 (Minority Quarantine) & INV-CHAOS-05 (State Convergence)"

go test -v -run "TestChaos_Partition_Case3_LeaderIsolatedMinority" ./internal/chaos
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`nPASS: Minority partition successfully quarantined and reconciled." -ForegroundColor Green
} else {
    Write-Host "`nFAIL: Minority partition violated safety invariant." -ForegroundColor Red
}

exit $exitCode
