
Function Get-RedirectedUrl {
  Param (
    [Parameter(Mandatory=$true)]
    [String]$URL
  )

  $request = [System.Net.WebRequest]::Create($url)
  $request.AllowAutoRedirect=$false
  $response = $request.GetResponse()
  If ($response.StatusCode -eq "Found" -or $response.StatusCode -eq "MovedPermanently") {
    $response.GetResponseHeader("Location")
  }
  Else {
    $URL
  }
}

Write-Host "Downloading Cosmos DB Emulator..."
$url = Get-RedirectedUrl "https://aka.ms/cosmosdb-emulator"
Invoke-WebRequest -Uri $url -OutFile "cosmosdb-emulator-install.msi"

Write-Host "Installing..."
$MSIArguments = @(
  "/i"
  "cosmosdb-emulator-install.msi"
  "/quiet"
  "/qn"
  "/norestart"
)
Start-Process "msiexec.exe" -ArgumentList $MSIArguments -Wait
