param(
  [ValidateSet('Debug', 'Release')]
  [string]$Configuration = 'Debug'
)

$ErrorActionPreference = 'Stop'

$distributedNodesDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoRoot = (Resolve-Path (Join-Path $distributedNodesDir '..')).Path
$configDir = $distributedNodesDir
$binaryDir = Join-Path $repoRoot "out\build\windows-x64\distributed-nodes\$Configuration"

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

function Test-NodeAlreadyRunning {
  param([string[]]$ProcessNames)

  foreach ($processName in $ProcessNames) {
    if (Get-Process -Name $processName -ErrorAction SilentlyContinue) {
      return $true
    }
  }

  return $false
}

$nodes = @(
  @{
    Name = 'master'
    Title = 'distributed-nodes-master'
    Exe = 'distributed-nodes-master.exe'
    Config = 'master.conf'
    Port = 47000
    DependsOnPort = $null
  },
  @{
    Name = 'region'
    Title = 'distributed-nodes-region'
    Exe = 'distributed-nodes-region.exe'
    Config = 'region-a.conf'
    Port = 47010
    DependsOnPort = 47000
  },
  @{
    Name = 'compute'
    Title = 'distributed-nodes-compute'
    Exe = 'distributed-nodes-compute.exe'
    Config = 'compute-a1.conf'
    Port = 47011
    DependsOnPort = 47010
  },
  @{
    Name = 'storage'
    Title = 'distributed-nodes-storage'
    Exe = 'distributed-nodes-storage.exe'
    Config = 'storage-a1.conf'
    Port = 47012
    DependsOnPort = 47010
  }
)

$processNames = $nodes | ForEach-Object { [System.IO.Path]::GetFileNameWithoutExtension($_.Exe) }
if (Test-NodeAlreadyRunning -ProcessNames $processNames) {
  throw 'distributed-nodes processes are already running. Stop them before starting a new cluster.'
}

foreach ($node in $nodes) {
  $exePath = Join-Path $binaryDir $node.Exe
  $configPath = Join-Path $configDir $node.Config

  if (-not (Test-Path $exePath)) {
    throw "Missing executable: $exePath"
  }

  if (-not (Test-Path $configPath)) {
    throw "Missing config file: $configPath"
  }

  if ($node.DependsOnPort -and -not (Wait-TcpPort -HostName '127.0.0.1' -Port $node.DependsOnPort)) {
    throw "Dependency port $($node.DependsOnPort) did not open before starting $($node.Name)."
  }

  $command = "Set-Variable -Scope Global -Name distributedNodesLauncher -Value '$($node.Title)'; `$Host.UI.RawUI.WindowTitle = '$($node.Title)'; & '$exePath' --config-file '$configPath'"

  Start-Process -FilePath 'powershell.exe' -WorkingDirectory $repoRoot -ArgumentList @(
    '-NoExit',
    '-ExecutionPolicy', 'Bypass',
    '-Command', $command
  ) | Out-Null

  if (-not (Wait-TcpPort -HostName '127.0.0.1' -Port $node.Port)) {
    throw "Node $($node.Name) did not start listening on port $($node.Port)."
  }

  Write-Host "Started $($node.Name) on port $($node.Port)."
}

Write-Host 'All distributed-nodes server nodes are running in dedicated PowerShell windows.'