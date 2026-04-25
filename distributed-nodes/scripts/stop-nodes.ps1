param()

$ErrorActionPreference = 'Stop'

$ports = 47000, 47010, 47011, 47012
$processNames = @(
  'distributed-nodes-master',
  'distributed-nodes-region',
  'distributed-nodes-compute',
  'distributed-nodes-storage'
)

$stopped = @()
foreach ($processName in $processNames) {
  $processes = Get-Process -Name $processName -ErrorAction SilentlyContinue
  if (-not $processes) {
    continue
  }

  foreach ($process in $processes) {
    Stop-Process -Id $process.Id -Force
    $stopped += "$($process.ProcessName)#$($process.Id)"
  }
}

$launcherShells = Get-CimInstance Win32_Process | Where-Object {
  $_.Name -eq 'powershell.exe' -and $_.CommandLine -like '*distributedNodesLauncher*'
}

foreach ($shell in $launcherShells) {
  Stop-Process -Id $shell.ProcessId -Force -ErrorAction SilentlyContinue
}

if ($stopped.Count -gt 0) {
  Write-Host ('Stopped node processes: ' + ($stopped -join ', '))
} else {
  Write-Host 'No distributed-nodes processes were running.'
}

$remaining = Get-NetTCPConnection -LocalPort $ports -ErrorAction SilentlyContinue
if ($remaining) {
  Write-Warning 'Some distributed-nodes ports are still in use:'
  $remaining | Select-Object LocalAddress, LocalPort, State, OwningProcess | Format-Table -AutoSize
} else {
  Write-Host 'All distributed-nodes ports are free.'
}