# scripts/chaos/run-all-chaos.ps1
# Runs the full reproducible Phase 8 Chaos Engineering suite.

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host " STARTING FULL CHAOS ENGINEERING VALIDATION SUITE (PHASE 8)" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

$startTime = Get-Date

go test -v -count=1 ./internal/chaos
$testStatus = $LASTEXITCODE

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host "================================================================================" -ForegroundColor Cyan
if ($testStatus -eq 0) {
    Write-Host " [PASS] ALL CHAOS EXPERIMENTS & INVARIANTS SATISFIED (Duration: $duration)" -ForegroundColor Green
} else {
    Write-Host " [FAIL] CHAOS EXPERIMENT SUITE FAILED (Duration: $duration)" -ForegroundColor Red
}
Write-Host "================================================================================" -ForegroundColor Cyan

exit $testStatus
