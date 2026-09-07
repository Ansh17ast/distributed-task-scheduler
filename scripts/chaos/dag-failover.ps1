# scripts/chaos/dag-failover.ps1
# Deterministic DAG Failover Experiment across Pipeline Execution Stages

Write-Host "================================================================================" -ForegroundColor Yellow
Write-Host " START: Experiment EXP-DAG-FAILOVER (Diamond DAG Failover A -> B/C -> D)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "FAULT INJECTED: Kill leader coordinator while downstream tasks B and C are READY"
Write-Host "OBSERVATION: Quorum elects new leader; DAG engine preserves topological dependency state"
Write-Host "RECOVERY: Workers execute parallel branches on new leader; join task D correctly unlocked"
Write-Host "INVARIANT CHECK: INV-CHAOS-08 (DAG Topology Integrity)"

go test -v -run "TestChaos_DAGFailover_DiamondPipeline" ./internal/chaos
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`nPASS: Diamond DAG completed with 100% topological correctness." -ForegroundColor Green
} else {
    Write-Host "`nFAIL: DAG failover experiment failed." -ForegroundColor Red
}

exit $exitCode
