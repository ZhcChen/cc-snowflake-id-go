$ErrorActionPreference = "Stop"

if (-not $env:IDGEN_TEST_DATABASE_URL) {
    throw "IDGEN_TEST_DATABASE_URL is required for integration tests."
}

$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $root
try {
    go test -tags=integration ./lease ./examples/lease-service
}
finally {
    Pop-Location
}
