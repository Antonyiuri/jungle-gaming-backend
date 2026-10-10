param(
    # How many events to print (at most 10 per call).
    [ValidateRange(1, 10)][int]$Count = 10
)

$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    docker compose run --rm --no-deps -T --entrypoint python provision /bootstrap/events.py $Count
    if ($LASTEXITCODE -ne 0) { throw 'Reading the events failed.' }
} finally {
    Pop-Location
}
