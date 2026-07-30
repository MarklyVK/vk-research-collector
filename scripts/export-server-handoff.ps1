[CmdletBinding()]
param(
    [string]$ServerUser,
    [string]$ServerHost,
    [string]$RemoteDirectory = "/opt/vk-research-collector/backups",
    [string]$RunId,
    [string]$OutputDirectory = "backups",
    [switch]$SkipUpload
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-DockerCompose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose завершился с кодом $LASTEXITCODE"
    }
}

function Get-EnvValue {
    param([string]$Name, [string]$Default)
    if (-not (Test-Path -LiteralPath ".env")) {
        return $Default
    }
    $match = Get-Content -LiteralPath ".env" -Encoding UTF8 |
        Where-Object { $_ -match "^$([regex]::Escape($Name))=" } |
        Select-Object -Last 1
    if ($null -eq $match) {
        return $Default
    }
    return ($match -split "=", 2)[1]
}

function Get-ClassificationCounts {
    $lines = Invoke-DockerCompose run --rm collector classification summary
    $counts = @{
        pending = 0
        approved = 0
        rejected = 0
        approved_by_label = [ordered]@{}
    }
    foreach ($line in $lines) {
        if ($line -match "^(Pending|Approved|Rejected):\s+(\d+)$") {
            $counts[$matches[1].ToLowerInvariant()] = [int]$matches[2]
        }
        elseif ($line -match "^\s{2}([a-z_]+):\s+(\d+)$") {
            $counts.approved_by_label[$matches[1]] = [int]$matches[2]
        }
    }
    return $counts
}

if ([string]::IsNullOrWhiteSpace($ServerUser) -xor [string]::IsNullOrWhiteSpace($ServerHost)) {
    throw "Для передачи укажите одновременно -ServerUser и -ServerHost."
}
if ($SkipUpload -and -not [string]::IsNullOrWhiteSpace($ServerHost)) {
    throw "-SkipUpload нельзя использовать вместе с -ServerUser/-ServerHost."
}

$previousErrorActionPreference = $ErrorActionPreference
try {
    $ErrorActionPreference = "Continue"
    & docker info 2> $null | Out-Null
    $dockerInfoExitCode = $LASTEXITCODE
}
finally {
    $ErrorActionPreference = $previousErrorActionPreference
}
if ($dockerInfoExitCode -ne 0) {
    throw "Docker не запущен."
}
Invoke-DockerCompose config --quiet

Write-Host "Останавливается локальный collector-worker..."
Invoke-DockerCompose stop collector-worker
$runningWorker = & docker compose ps --status running -q collector-worker
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($runningWorker -join ""))) {
    throw "Локальный collector-worker не остановлен."
}

$statusArguments = @("run", "--rm", "collector", "collection", "status")
if (-not [string]::IsNullOrWhiteSpace($RunId)) {
    $statusArguments += @("--run-id", $RunId)
}
$statusText = (Invoke-DockerCompose @statusArguments) -join "`n"
$collectionStatus = $statusText | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = [string]$collectionStatus.run_id
}
if ([string]::IsNullOrWhiteSpace($RunId)) {
    throw "Collection run ID не найден; укажите -RunId."
}

$classification = Get-ClassificationCounts
$alembicText = (Invoke-DockerCompose run --rm collector alembic current) -join "`n"
$alembicRevision = if ($alembicText -match "(?m)^([0-9]{8}_[0-9]{4})\s") {
    $matches[1]
}
else {
    throw "Не удалось определить текущую ревизию Alembic."
}
$database = Get-EnvValue -Name "POSTGRES_DB" -Default "vk_research"
$databaseUser = Get-EnvValue -Name "POSTGRES_USER" -Default "vk_collector"
$timestamp = [DateTime]::UtcNow.ToString("yyyyMMdd-HHmmssZ")
$baseName = "server-handoff-$timestamp-$RunId"
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
$dumpPath = Join-Path $outputPath "$baseName.dump"
$manifestPath = Join-Path $outputPath "$baseName.manifest.json"

if ((Test-Path -LiteralPath $dumpPath) -or (Test-Path -LiteralPath $manifestPath)) {
    throw "Handoff-файлы уже существуют; перезапись запрещена: $baseName"
}
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null

$containerDump = "/tmp/$baseName.dump"
try {
    Invoke-DockerCompose exec -T postgres pg_dump -U $databaseUser -d $database -Fc -f $containerDump
    & docker compose cp "postgres:$containerDump" $dumpPath
    if ($LASTEXITCODE -ne 0) {
        throw "Не удалось скопировать dump из PostgreSQL container."
    }
}
finally {
    & docker compose exec -T postgres rm -f -- $containerDump *> $null
}

$dump = Get-Item -LiteralPath $dumpPath
if ($dump.Length -le 0) {
    throw "Создан пустой dump."
}
$mount = "${outputPath}:/handoff:ro"
& docker run --rm -v $mount postgres:16-alpine pg_restore --list "/handoff/$($dump.Name)" *> $null
if ($LASTEXITCODE -ne 0) {
    throw "pg_restore --list отклонил dump."
}

$sha256 = (Get-FileHash -LiteralPath $dumpPath -Algorithm SHA256).Hash.ToLowerInvariant()
$manifest = [ordered]@{
    created_at = [DateTime]::UtcNow.ToString("o")
    dump_file = $dump.Name
    sha256 = $sha256
    database = $database
    run_id = $RunId
    alembic_revision = $alembicRevision
    classification = [ordered]@{
        approved = $classification.approved
        rejected = $classification.rejected
        pending = $classification.pending
        approved_by_label = $classification.approved_by_label
    }
    collection_status = $collectionStatus
}
$manifest | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

if (-not $SkipUpload -and -not [string]::IsNullOrWhiteSpace($ServerHost)) {
    $remote = "${ServerUser}@${ServerHost}:$RemoteDirectory/"
    & scp -- $dumpPath $manifestPath $remote
    if ($LASTEXITCODE -ne 0) {
        throw "scp не завершил передачу handoff. Локальный worker остаётся остановлен."
    }
    Write-Host "Handoff передан в $remote"
}

Write-Host "Dump: $dumpPath"
Write-Host "Manifest: $manifestPath"
Write-Host "SHA256: $sha256"
Write-Warning "Локальный collector-worker оставлен остановленным. Не запускайте его одновременно с серверным worker."
