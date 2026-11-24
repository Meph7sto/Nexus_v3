$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

$tag = [guid]::NewGuid().ToString('N').Substring(0, 12)
$project = "nexus-backup-rehearsal-$tag"
$env:NEXUS_REHEARSAL_DB = "nexus_backup_rehearsal_$tag"
$env:NEXUS_REHEARSAL_SECRET = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24)).ToLowerInvariant()
$compose = Join-Path $PSScriptRoot 'backup.compose.yaml'
$backend = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../../../../backend')).Path
$composeArgs = @('compose', '--project-name', $project, '-f', $compose)
$setEnv = @('NEXUS_REHEARSAL_DB', 'NEXUS_REHEARSAL_SECRET', 'NEXUS_REHEARSAL_OLD_PG',
    'NEXUS_REHEARSAL_NEW_PG', 'NEXUS_REHEARSAL_OLD_REDIS', 'NEXUS_REHEARSAL_NEW_REDIS', 'NEXUS_REHEARSAL_S3')

$dumpVersion = pg_dump --version
$restoreVersion = psql --version
if ($dumpVersion -notmatch 'PostgreSQL\) 18\.' -or $restoreVersion -notmatch 'PostgreSQL\) 18\.') {
    throw 'The local rehearsal requires pg_dump and psql from PostgreSQL 18.'
}

function Get-RehearsalPort([string] $service, [string] $port) {
    $configured = docker @composeArgs port $service $port
    if ($configured -notmatch '^127\.0\.0\.1:(\d+)$') {
        throw 'A rehearsal service is not bound exclusively to IPv4 loopback.'
    }
    return $Matches[1]
}

try {
    docker @composeArgs up -d --pull never
    $env:NEXUS_REHEARSAL_OLD_PG = Get-RehearsalPort 'old-pg' '5432'
    $env:NEXUS_REHEARSAL_NEW_PG = Get-RehearsalPort 'new-pg' '5432'
    $env:NEXUS_REHEARSAL_OLD_REDIS = Get-RehearsalPort 'old-redis' '6379'
    $env:NEXUS_REHEARSAL_NEW_REDIS = Get-RehearsalPort 'new-redis' '6379'
    $env:NEXUS_REHEARSAL_S3 = Get-RehearsalPort 's3' '9090'

    Push-Location -LiteralPath $backend
    try {
        go test -tags=backup_rehearsal -count=1 -timeout=4m ./internal/repository -run '^TestDataBackupRehearsal$' -v
    } finally {
        Pop-Location
    }
} finally {
    try {
        docker @composeArgs down --volumes
    } finally {
        foreach ($key in $setEnv) {
            Remove-Item -LiteralPath "Env:$key" -ErrorAction SilentlyContinue
        }
    }
}
#Requires -Version 7.4
