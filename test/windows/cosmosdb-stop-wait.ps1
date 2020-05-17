
Push-Location -Path "$Env:Programfiles\Azure Cosmos DB Emulator"

Start-Process -FilePath ".\Microsoft.Azure.Cosmos.Emulator.exe" -Args "/Shutdown"

Write-Host "Waiting for Cosmos DB to stop..."
do {
  Start-Sleep -Seconds 1
  $process = (Start-Process -FilePath ".\Microsoft.Azure.Cosmos.Emulator.exe" -Args "/GetStatus" -PassThru -Wait)
} while($process.ExitCode -ne 3)

Pop-Location
