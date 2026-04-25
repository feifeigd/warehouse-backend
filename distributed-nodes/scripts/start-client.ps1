param(
  [ValidateSet('Debug', 'Release')]
  [string]$Configuration = 'Debug',
  [string]$MasterHost = '127.0.0.1',
  [int]$MasterPort = 47000
)

$ErrorActionPreference = 'Stop'

$distributedNodesDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoRoot = (Resolve-Path (Join-Path $distributedNodesDir '..')).Path
$configDir = $distributedNodesDir
$binaryDir = Join-Path $repoRoot "out\build\windows-x64\distributed-nodes\$Configuration"
$exePath = Join-Path $binaryDir 'distributed-nodes-client.exe'
$configPath = Join-Path $configDir 'client.conf'

function Wait-TcpPort {
  param(
    [string]$HostName,
    [int]$Port,
    [int]$TimeoutSeconds = 10
  )

  $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
  while ([DateTime]::UtcNow -lt $deadline) {
    $client = $null
    try {
      $client = [System.Net.Sockets.TcpClient]::new()
      $asyncResult = $client.BeginConnect($HostName, $Port, $null, $null)
      if ($asyncResult.AsyncWaitHandle.WaitOne(250) -and $client.Connected) {
        $client.EndConnect($asyncResult)
        return $true
      }
    } catch {
      # Retry until timeout.
    } finally {
      if ($client) {
        $client.Dispose()
      }
    }
    Start-Sleep -Milliseconds 250
  }

  return $false
}

if (-not (Test-Path $exePath)) {
  throw "Missing executable: $exePath"
}

if (-not (Test-Path $configPath)) {
  throw "Missing config file: $configPath"
}

if (-not (Wait-TcpPort -HostName $MasterHost -Port $MasterPort)) {
  throw "Master node is not reachable at ${MasterHost}:$MasterPort. Start the server nodes first."
}

Write-Host "Running distributed-nodes client against ${MasterHost}:$MasterPort..."
& $exePath --config-file $configPath