#Requires -Version 7.4

param(
    [switch] $IncludeV2,
    [switch] $PrepareOnly,
    [switch] $CapacitySample
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$tag = [guid]::NewGuid().ToString('N').Substring(0, 12)
$repo = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../../../..')).Path
$tmpRoot = (Resolve-Path -LiteralPath ([IO.Path]::GetTempPath())).Path
$work = Join-Path $tmpRoot "nexus-chain-rehearsal-$tag"
New-Item -ItemType Directory -Path $work | Out-Null
$oldEnv = @{}
foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED', 'GOPROXY', 'NEXUS_CHAIN_DIR', 'NEXUS_CHAIN_BINARY', 'NEXUS_CHAIN_RELAY', 'NEXUS_CHAIN_MODE', 'NEXUS_CHAIN_CLUSTER', 'NEXUS_CHAIN_ROLE', 'NEXUS_CHAIN_CONCURRENCY')) {
    $oldEnv[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$env:NEXUS_CHAIN_DIR = $work
$env:GOPROXY = 'off'
$env:CGO_ENABLED = '0'
$compose = @('compose', '--project-name', "nexus-chain-rehearsal-$tag", '-f', (Join-Path $PSScriptRoot 'chain.compose.yaml'))
if ($CapacitySample) {
    $IncludeV2 = $true
    $compose += @('-f', (Join-Path $PSScriptRoot 'chain-capacity.compose.yaml'))
}
$fixture = Join-Path $PSScriptRoot 'chainfixture/main.go'
$started = $false

function Invoke-RehearsalFault([string] $mode, [string[]] $permitted) {
    $driver = "nexus-chain-rehearsal-$tag-$mode"
    $env:NEXUS_CHAIN_MODE = $mode
    docker @compose run --no-deps -d -T --pull never --name $driver driver | Out-Null
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $node = ''
    while ($clock.Elapsed.TotalSeconds -lt 48) {
        $message = (docker logs $driver) -join "`n"
        if ($message -cmatch '(?m)^REHEARSAL_ACTIVE_NODE=(entry|worker-a|worker-b)\s*$') {
            $node = $Matches[1]
            break
        }
        if ((docker inspect --format '{{.State.Running}}' $driver) -ne 'true') { break }
        Start-Sleep -Milliseconds 120
    }
    if ($node -notin $permitted) {
        docker logs $driver
        throw 'The fault driver did not identify a permitted active node.'
    }
    if ($mode -eq 'entry-graceful') {
        docker @compose stop --timeout 30 $node
    } else {
        docker @compose kill -s SIGKILL $node
    }
    $exitCode = docker wait $driver
    docker logs $driver
    if ($exitCode -ne '0') { throw "Fault rehearsal failed: $mode" }
    docker rm $driver | Out-Null
}

function Invoke-RehearsalCapacity([string] $mode, [int] $n) {
    $env:NEXUS_CHAIN_MODE = $mode
    $env:NEXUS_CHAIN_CONCURRENCY = [string] $n
    $go = Join-Path $work 'capacity-go'
    $done = Join-Path $work 'capacity-done'
    foreach ($signal in @($go, $done)) {
        if (Test-Path -LiteralPath $signal) {
            Remove-Item -LiteralPath $signal
        }
    }
    docker @compose up -d --pull never --force-recreate fixture worker-a worker-b entry
    $driver = "nexus-chain-rehearsal-$tag-$mode-$n"
    docker @compose run --no-deps -d -T --pull never --name $driver driver | Out-Null
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $ready = $false
    while ($clock.Elapsed.TotalSeconds -lt 50) {
        if ((docker logs $driver) -contains 'REHEARSAL_CAPACITY_READY') {
            $ready = $true
            break
        }
        if ((docker inspect --format '{{.State.Running}}' $driver) -ne 'true') { break }
        Start-Sleep -Milliseconds 120
    }
    if (-not $ready) {
        docker logs $driver
        throw 'The capacity driver did not report readiness.'
    }

    $targets = [ordered]@{}
    foreach ($node in @('entry', 'worker-a', 'worker-b')) {
        $id = docker @compose ps -q $node
        if ($id -notmatch '^[0-9a-f]{12,64}$') { throw "Unexpected test container ID for $node." }
        $limits = docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' $id
        if ($limits -ne '1000000000 1073741824') { throw "Wrong capacity limits for $node." }
        if ($node -eq 'entry' -or $mode -eq 'v3-relay') {
            $targets[$node] = $id
            docker exec -d $id sh /rehearsal/capacity-rss.sh
        }
    }
    New-Item -ItemType File -Path $go | Out-Null
    try {
        $exitCode = docker wait $driver
    } finally {
        New-Item -ItemType File -Path $done | Out-Null
    }
    docker logs $driver
    if ($exitCode -ne '0') { throw "Capacity sample failed: $mode/$n" }
    Start-Sleep -Milliseconds 350
    foreach ($node in $targets.Keys) {
        $lines = @(docker exec $targets[$node] cat /tmp/nexus-rehearsal-rss)
        $idle = $null
        $peak = 0
        $hwm = 0
        $samples = 0
        foreach ($line in $lines) {
            if ($line -match '^(idle|active|end) (\d+) (\d+)$') {
                $rss = [int] $Matches[2]
                $hwm = [Math]::Max($hwm, [int] $Matches[3])
                if ($Matches[1] -eq 'idle') { $idle = $rss }
                if ($Matches[1] -eq 'active') {
                    $peak = [Math]::Max($rss, $peak)
                    $samples++
                }
            }
        }
        if ($null -eq $idle -or $samples -lt 10) { throw "Insufficient RSS samples for $node." }
        Write-Output "CAPACITY_MEM mode=$mode offered=$n node=$node idle_rss_kib=$idle active_peak_rss_kib=$peak vm_hwm_kib=$hwm samples=$samples"
    }
    docker rm $driver | Out-Null
    docker @compose stop entry
}

try {
    $env:GOOS = 'windows'
    $env:GOARCH = 'amd64'
    $generate = Join-Path $work 'certgen.exe'
    go build -o $generate $fixture
    & $generate -generate -dir $work

    $env:GOOS = 'linux'
    go build -trimpath -ldflags '-s -w' -o (Join-Path $work 'chainfixture') $fixture
    Push-Location -LiteralPath (Join-Path $repo 'backend')
    try {
        go build -buildvcs=false -trimpath -ldflags '-s -w' -o (Join-Path $work 'entry-v3') ./cmd/entry
        go build -buildvcs=false -trimpath -ldflags '-s -w' -o (Join-Path $work 'worker') ./cmd/worker
        go test -buildvcs=false -c -tags=full_chain_rehearsal -o (Join-Path $work 'chain-driver') ./internal/repository
        Copy-Item -LiteralPath 'resources/model-pricing/model_prices_and_context_window.json' -Destination (Join-Path $work 'model-pricing.json')
        if ($CapacitySample) {
            $capture = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'capacity-rss.sh') -Raw
            [IO.File]::WriteAllText((Join-Path $work 'capacity-rss.sh'), $capture.Replace("`r`n", "`n"))
        }
    } finally {
        Pop-Location
    }

    if ($IncludeV2) {
        $baseline = '57035de2ef0fd5e54984e3648825d9948bab6ffa'
        $source = (Resolve-Path -LiteralPath (Join-Path $repo '../Nexus_v2')).Path
        $archive = Join-Path $work 'v2.tar'
        $v2 = Join-Path $work 'v2'
        New-Item -ItemType Directory -Path $v2 | Out-Null
        $trustedSource = $source.Replace('\', '/')
        git -c "safe.directory=$trustedSource" -C $source archive --format=tar "--output=$archive" $baseline backend
        tar -xf $archive -C $v2
        Push-Location -LiteralPath (Join-Path $v2 'backend')
        try {
            go build -buildvcs=false -trimpath -ldflags '-s -w' -o (Join-Path $work 'entry-v2') ./cmd/server
        } finally {
            Pop-Location
        }
    }

    if ($PrepareOnly) {
        Write-Output 'Built the isolated rehearsal fixtures without starting Docker.'
        return
    }

    $started = $true
    $env:NEXUS_CHAIN_CLUSTER = 'false'
    $env:NEXUS_CHAIN_ROLE = 'all'
    $env:NEXUS_CHAIN_CONCURRENCY = '0'
    docker @compose up -d --pull never postgres redis fixture worker-a worker-b
    $env:NEXUS_CHAIN_MODE = if ($CapacitySample) { 'setup-capacity' } else { 'setup' }
    docker @compose run --rm -T --pull never driver

    $variants = @('v3-local', 'v3-relay')
    if ($IncludeV2) { $variants = @('v2') + $variants }
    foreach ($variant in $variants) {
        $env:NEXUS_CHAIN_BINARY = if ($variant -eq 'v2') { 'v2' } else { 'v3' }
        $env:NEXUS_CHAIN_RELAY = if ($variant -eq 'v3-relay') { 'true' } else { 'false' }
        if ($CapacitySample) {
            Invoke-RehearsalCapacity $variant 4
            Invoke-RehearsalCapacity $variant 8
            continue
        }
        $env:NEXUS_CHAIN_MODE = $variant
        docker @compose up -d --pull never --force-recreate entry
        docker @compose run --rm -T --pull never driver
        docker @compose stop entry
    }
    if ($CapacitySample) { return }

    $env:NEXUS_CHAIN_BINARY = 'v3'
    $env:NEXUS_CHAIN_RELAY = 'true'
    $env:NEXUS_CHAIN_CLUSTER = 'true'
    $env:NEXUS_CHAIN_ROLE = 'gateway_only'
    docker @compose --profile entry-failover up -d --pull never --force-recreate entry entry-peer
    Invoke-RehearsalFault 'entry-failover' @('entry')
    docker @compose --profile entry-failover stop entry-peer

    $env:NEXUS_CHAIN_CLUSTER = 'false'
    $env:NEXUS_CHAIN_ROLE = 'all'
    docker @compose up -d --pull never --force-recreate entry
    Invoke-RehearsalFault 'entry-graceful' @('entry')
    docker @compose up -d --pull never --force-recreate entry
    Invoke-RehearsalFault 'worker-failover' @('worker-a', 'worker-b')
} catch {
    if ($started) {
        docker @compose --profile entry-failover logs --tail 70 entry entry-peer fixture worker-a worker-b
    }
    throw
} finally {
    try {
        if ($started) { docker @compose --profile entry-failover down --volumes --remove-orphans }
    } finally {
        $actual = (Resolve-Path -LiteralPath $work).Path
        if ((Split-Path -Parent $actual) -eq $tmpRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) -and
            (Split-Path -Leaf $actual) -eq "nexus-chain-rehearsal-$tag") {
            Remove-Item -LiteralPath $actual -Recurse -Force
        }
        foreach ($name in $oldEnv.Keys) {
            [Environment]::SetEnvironmentVariable($name, $oldEnv[$name], 'Process')
        }
    }
}
