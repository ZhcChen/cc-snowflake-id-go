$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $root
try {
    go test ./...
}
finally {
    Pop-Location
}
