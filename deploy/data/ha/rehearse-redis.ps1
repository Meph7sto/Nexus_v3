param(
  [string]$Context = 'desktop-linux',
  [string]$ProbeBinary
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$project = 'nexus-v3-redis-drill-' + [Guid]::NewGuid().ToString('N').Substring(0, 10)
$temp = Join-Path $root ".cache/$project"
New-Item -Path $temp -ItemType Directory -Force | Out-Null
$env:HA_REDIS_PASSWORD_FILE = Join-Path $temp 'data.secret'
$env:HA_REDIS_SENTINEL_PASSWORD_FILE = Join-Path $temp 'sentinel.secret'
[IO.File]::WriteAllText($env:HA_REDIS_PASSWORD_FILE, [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)))
[IO.File]::WriteAllText($env:HA_REDIS_SENTINEL_PASSWORD_FILE, [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)))
$networkFile = Join-Path $temp 'network.yaml'
[IO.File]::WriteAllText($networkFile, "networks:`n  ha:`n    internal: true`n")

function Run-Docker {
  & docker --context $Context @args
  if ($LASTEXITCODE -ne 0) { throw "Docker command failed ($LASTEXITCODE)" }
}

function Wait-ForLog([string]$marker, [int]$seconds) {
  $deadline = (Get-Date).AddSeconds($seconds)
  $exited = $false
  do {
    $logs = Run-Docker logs $probe 2>&1 | Out-String
    if ($logs.Contains($marker)) {
      ($logs -split "`n" | Where-Object { $_.Contains($marker) } | Select-Object -Last 1).Trim()
      return
    }
    if ($exited) { break }
    $exited = (Run-Docker inspect -f '{{.State.Running}}' $probe) -ne 'true'
    Start-Sleep -Milliseconds 350
  } while ((Get-Date) -lt $deadline)
  Run-Docker logs $probe
  throw "Drill ended before $marker"
}

$compose = @('compose', '-p', $project, '-f', (Join-Path $PSScriptRoot 'redis.compose.yaml'), '-f', $networkFile)
$probe = "$project-probe"
$probeStarted = $false
$servicesStarted = $false
$paused = $false
try {
  if (-not $ProbeBinary) {
    $ProbeBinary = Join-Path $temp 'redis-drill.test'
    Push-Location (Join-Path $root 'backend')
    try {
      $env:GOOS = 'linux'
      $env:GOARCH = 'amd64'
      $env:CGO_ENABLED = '0'
      & go test -c -o $ProbeBinary ./internal/repository
      if ($LASTEXITCODE -ne 0) { throw 'Redis drill compile failed' }
    } finally {
      Pop-Location
    }
  }
  $ProbeBinary = (Resolve-Path $ProbeBinary).Path
  $servicesStarted = $true
  Run-Docker @compose up -d
  $probeArgs = @(
    'run', '--detach', '--name', $probe, '--network', "${project}_ha",
    '--env', 'NEXUS_REDIS_SENTINEL_DRILL=1',
    '--mount', "type=bind,source=$ProbeBinary,target=/drill.test,readonly",
    '--mount', "type=bind,source=$env:HA_REDIS_PASSWORD_FILE,target=/run/secrets/redis_data_password,readonly",
    '--mount', "type=bind,source=$env:HA_REDIS_SENTINEL_PASSWORD_FILE,target=/run/secrets/redis_sentinel_password,readonly",
    'redis:8-alpine', 'sh', '-ec',
    'cp /drill.test /tmp/drill.test; chmod +x /tmp/drill.test; exec /tmp/drill.test -test.v -test.run ^TestRedisSentinelIsolatedDrill$ -test.timeout 105s'
  )
  Run-Docker @probeArgs
  $probeStarted = $true
  Wait-ForLog 'REDIS_DRILL_READY' 30
  # Unreachable process while retaining its Docker hostname, as on a private
  # network where DNS does not disappear when the Redis process fails.
  Run-Docker @compose pause redis-a
  $paused = $true
  Wait-ForLog 'REDIS_DRILL_PROMOTED' 45
  Wait-ForLog 'REDIS_DRILL_WAIT_FOR_REJOIN' 10
  Run-Docker @compose unpause redis-a
  $paused = $false
  Wait-ForLog 'REDIS_DRILL_WRITABLE' 40
  Wait-ForLog 'REDIS_DRILL_VERIFIED' 12
  $exitCode = Run-Docker wait $probe
  if ($exitCode -ne '0') {
    Run-Docker logs $probe
    throw "Redis drill probe exited $exitCode"
  }
} catch {
  if ($servicesStarted) { & docker --context $Context @compose logs --tail 30 redis-sentinel-a redis-sentinel-b redis-sentinel-c }
  throw
} finally {
  if ($paused) { & docker --context $Context @compose unpause redis-a }
  if ($probeStarted) { & docker --context $Context rm -f $probe | Out-Null }
  if ($servicesStarted) { & docker --context $Context @compose down -v }
  Remove-Item -LiteralPath $env:HA_REDIS_PASSWORD_FILE,$env:HA_REDIS_SENTINEL_PASSWORD_FILE -ErrorAction SilentlyContinue
}
