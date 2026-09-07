# scripts/chaos/follower-restart.ps1
# Deterministic Follower Termination & Rejoin Experiment

Write-Host "================================================================================" -ForegroundColor Yellow
Write-Host " START: Experiment EXP-FOLLOWER-KILL (Follower Termination & Log Catchup)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "FAULT INJECTED: Terminate follower; commit tasks to remaining 2-node quorum"
Write-Host "OBSERVATION: Cluster continues uninterrupted; leader accepts mutations"
Write-Host "RECOVERY: Follower restarted; replays logs; converges to leader FSM"
Write-Host "INVARIANT CHECK: INV-CHAOS-05 (Eventual State Convergence)"

go test -v -run "TestChaos_FollowerKillAndRejoin" ./internal/chaos
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "`nPASS: Follower restarted and caught up without leader disruption." -ForegroundColor Green
} else {
    Write-Host "`nFAIL: Follower restart experiment failed." -ForegroundColor Red
}

exit $exitCode
