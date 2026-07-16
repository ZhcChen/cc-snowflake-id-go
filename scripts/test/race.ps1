$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $root
try {
    $env:CGO_ENABLED = "1"
    go test -race ./...
}
finally {
    Pop-Location
}
