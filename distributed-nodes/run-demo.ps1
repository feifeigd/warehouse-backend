param(
  [ValidateSet('Debug', 'Release')]
  [string]$Configuration = 'Debug',
  [string]$MasterHost = '127.0.0.1',
  [int]$MasterPort = 47000,
  [switch]$KeepNodesRunning
)

$ErrorActionPreference = 'Stop'

$startNodesScript = Join-Path $PSScriptRoot 'start-nodes.ps1'
$startClientScript = Join-Path $PSScriptRoot 'start-client.ps1'
$stopNodesScript = Join-Path $PSScriptRoot 'stop-nodes.ps1'

$nodesStarted = $false

try {
  Write-Host 'Starting distributed-nodes server cluster...'
  & $startNodesScript -Configuration $Configuration
  $nodesStarted = $true

  Write-Host ''
  Write-Host 'Running distributed-nodes demo client...'
  & $startClientScript -Configuration $Configuration -MasterHost $MasterHost -MasterPort $MasterPort
}
finally {
  if ($nodesStarted -and -not $KeepNodesRunning) {
    Write-Host ''
    Write-Host 'Stopping distributed-nodes server cluster...'
    & $stopNodesScript
  }
}

if ($KeepNodesRunning) {
  Write-Host ''
  Write-Host 'Demo finished. Server nodes are still running because -KeepNodesRunning was specified.'
} else {
  Write-Host ''
  Write-Host 'Demo finished. Server nodes were stopped automatically.'
}