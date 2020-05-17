
Push-Location -Path "$Env:Programfiles\Azure Cosmos DB Emulator"

Start-Process -FilePath ".\Microsoft.Azure.Cosmos.Emulator.exe" -Args "/EnableCassandraEndpoint","/EnableRateLimiting","/NoExplorer","/NoUI"

Write-Host "Waiting for Cosmos DB to start..."
$lastExitCode = 0
Do {
  Start-Sleep -Seconds 1
  $process = (Start-Process -FilePath ".\Microsoft.Azure.Cosmos.Emulator.exe" -Args "/GetStatus" -PassThru -Wait)
  If ($process.ExitCode -ne $lastExitCode) {
    Write-Host "GetStatus code =" $process.ExitCode
    $lastExitCode = $process.ExitCode
  }
  If ($lastExitCode -eq 3 -or $lastExitCode -lt 0) {
    Pop-Location
    Exit 1
  }
} While ($process.ExitCode -ne 2)

Pop-Location
