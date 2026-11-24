# Creates uniquely named local resources and synthetic identities, then removes
# only this project's containers/volumes and its own certificate directory.
[CmdletBinding()]
param(
    [ValidateSet("sync", "sync_strict", "async")]
    [string]$Mode = "sync",
    [string]$GoPath = "go",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$project = "nexushapg-" + [Guid]::NewGuid().ToString("N").Substring(0, 12)
$parent = Join-Path $PSScriptRoot ".rehearsal"
$secrets = Join-Path $parent $project
$composeFile = Join-Path $PSScriptRoot "compose.yaml"
$backend = (Resolve-Path (Join-Path $PSScriptRoot "../../../backend")).Path
$compose = @("compose", "-f", $composeFile, "-p", $project)
$envNames = @("HA_SECRETS_DIR", "HA_ETCD_CLUSTER_TOKEN", "HA_PG_MODE", "HA_SCOPE", "HA_ENTRY_PG_PORT", "HA_PATRONI_IMAGE",
    "HA_PG1_HOST", "HA_PG2_HOST", "HA_ETCD1_HOST", "HA_ETCD2_HOST", "HA_ETCD3_HOST",
    "NEXUS_HA_REHEARSAL_PROJECT", "NEXUS_HA_REHEARSAL_COMPOSE", "NEXUS_HA_REHEARSAL_SECRETS", "NEXUS_HA_REHEARSAL_MODE", "OPENSSL_CONF")
$prior = @{}
foreach ($name in $envNames) {
    $prior[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

function Invoke-Checked([string]$Program, [string[]]$Arguments) {
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Program exited with code $LASTEXITCODE" }
}

function New-Certificate([string]$Name, [string]$Folder, [string]$Names, [string]$Usage) {
    $key = Join-Path $Folder "server.key"
    $cert = Join-Path $Folder "server.crt"
    $request = Join-Path $Folder "request.csr"
    $reqArgs = @("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
        "-subj", "/CN=$Name", "-keyout", $key, "-out", $request, "-addext", "extendedKeyUsage=$Usage")
    if ($Names) { $reqArgs += @("-addext", "subjectAltName=$Names") }
    Invoke-Checked "openssl" $reqArgs
    Invoke-Checked "openssl" @("x509", "-req", "-in", $request, "-CA", (Join-Path $secrets "ca.crt"),
        "-CAkey", (Join-Path $secrets "ca.key"), "-CAcreateserial", "-copy_extensions", "copy", "-days", "2", "-out", $cert)
    Copy-Item -LiteralPath (Join-Path $secrets "ca.crt") -Destination (Join-Path $Folder "ca.crt")
}

$started = $false
try {
    $existing = & docker ps -aq --filter "label=com.docker.compose.project=$project"
    if ($LASTEXITCODE -ne 0 -or $existing) { throw "Docker unavailable or rehearsal project already exists" }
    $existingVolumes = & docker volume ls -q --filter "label=com.docker.compose.project=$project"
    if ($LASTEXITCODE -ne 0 -or $existingVolumes) { throw "Rehearsal project already has persistent data" }
    $existingNetworks = & docker network ls -q --filter "label=com.docker.compose.project=$project"
    if ($LASTEXITCODE -ne 0 -or $existingNetworks) { throw "Rehearsal project already has a network" }

    New-Item -ItemType Directory -Path $secrets -Force | Out-Null
    $env:OPENSSL_CONF = Join-Path $PSScriptRoot "openssl-rehearsal.cnf"
    Invoke-Checked "openssl" @("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
        "-nodes", "-days", "2", "-subj", "/CN=Nexus local HA rehearsal",
        "-keyout", (Join-Path $secrets "ca.key"), "-out", (Join-Path $secrets "ca.crt"),
        "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    foreach ($name in @("etcd-1", "etcd-2", "etcd-3", "data-1", "data-2", "haproxy", "patroni-common")) {
        New-Item -ItemType Directory -Path (Join-Path $secrets $name) | Out-Null
    }
    foreach ($name in @("etcd-1", "etcd-2", "etcd-3")) {
        New-Certificate $name (Join-Path $secrets $name) "DNS:$name" "serverAuth,clientAuth"
    }
    foreach ($name in @("data-1", "data-2")) {
        New-Certificate $name (Join-Path $secrets $name) "DNS:$name,DNS:pg-proxy,DNS:localhost,IP:127.0.0.1" "serverAuth,clientAuth"
    }
    $haproxy = Join-Path $secrets "haproxy"
    New-Certificate "entry-rehearsal" $haproxy "" "clientAuth"
    $pem = [IO.File]::ReadAllText((Join-Path $haproxy "server.crt")) + [IO.File]::ReadAllText((Join-Path $haproxy "server.key"))
    [IO.File]::WriteAllText((Join-Path $haproxy "client.pem"), $pem)
    foreach ($name in @("postgres-password", "replicator-password", "rewind-password", "rest-password")) {
        [IO.File]::WriteAllText((Join-Path $secrets "patroni-common/$name"), [Guid]::NewGuid().ToString("N"))
    }
    $env:HA_SECRETS_DIR = $secrets.Replace("\", "/")
    $env:HA_ETCD_CLUSTER_TOKEN = $project
    $env:HA_PG_MODE = $Mode.ToLowerInvariant()
    $env:HA_SCOPE = $project
    $env:HA_ENTRY_PG_PORT = "0"
    $env:HA_PATRONI_IMAGE = "nexus-patroni18:4.1.1"
    $env:HA_PG1_HOST = "data-1"
    $env:HA_PG2_HOST = "data-2"
    $env:HA_ETCD1_HOST = "etcd-1"
    $env:HA_ETCD2_HOST = "etcd-2"
    $env:HA_ETCD3_HOST = "etcd-3"
    if (-not $NoBuild) {
        Invoke-Checked "docker" ($compose + @("build", "data-1"))
    }
    Invoke-Checked "docker" ($compose + @("config", "--quiet"))
    $started = $true
    Invoke-Checked "docker" ($compose + @("up", "-d", "--no-build", "--pull", "never"))

    $env:NEXUS_HA_REHEARSAL_PROJECT = $project
    $env:NEXUS_HA_REHEARSAL_COMPOSE = $composeFile
    $env:NEXUS_HA_REHEARSAL_SECRETS = $secrets
    $env:NEXUS_HA_REHEARSAL_MODE = $env:HA_PG_MODE
    Push-Location $backend
    try {
        Invoke-Checked $GoPath @("run", (Join-Path $PSScriptRoot "rehearse_pool.go"))
    } finally {
        Pop-Location
    }
} catch {
    if ($started) {
        & docker @compose ps -a
        & docker @compose logs --tail 20 data-1 data-2 pg-proxy etcd-1
    }
    throw
} finally {
    if ($started) {
        & docker @compose down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { Write-Warning "Review and clean the isolated project $project manually." }
    }
    if (Test-Path -LiteralPath $secrets) {
        $actual = (Resolve-Path -LiteralPath $secrets).Path
        $expected = Join-Path (Resolve-Path -LiteralPath $parent).Path $project
        if ($actual -ne $expected -or (Split-Path -Leaf $actual) -notmatch "^nexushapg-[0-9a-f]{12}$") {
            throw "Refusing to remove an unexpected rehearsal directory"
        }
        Remove-Item -LiteralPath $actual -Recurse -Force
    }
    foreach ($name in $envNames) {
        [Environment]::SetEnvironmentVariable($name, $prior[$name], "Process")
    }
}
