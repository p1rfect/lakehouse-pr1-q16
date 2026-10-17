$ErrorActionPreference='Stop'
$taskRoot=Split-Path $PSScriptRoot -Parent
$taskResultDir=Join-Path $taskRoot 'results'
New-Item -ItemType Directory -Path $taskResultDir -Force | Out-Null
$taskSystem=[ordered]@{
    Cpu=(Get-CimInstance Win32_Processor).Name
    HostMemoryBytes=(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    DockerMemoryBytes=[long](docker info --format '{{.MemTotal}}')
    DockerCPUs=[int](docker info --format '{{.NCPU}}')
    FreeDiskBytes=(Get-PSDrive -Name ([IO.Path]::GetPathRoot($taskRoot).Substring(0,1))).Free
    DateMoscow=[DateTime]::UtcNow.AddHours(3).ToString('yyyy-MM-dd')
    CapturedUtc=[DateTime]::UtcNow.ToString('o')
}
$taskSystem | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $taskResultDir 'system.json') -Encoding UTF8
