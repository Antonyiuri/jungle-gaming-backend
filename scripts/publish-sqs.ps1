param(
    [Parameter(Mandatory = $true)][string]$EnvelopePath,
    [ValidateSet('provider-a', 'provider-b')][string]$Provider = 'provider-a',
    # Keycloak pins its public hostname, so the issuer claim is idp.localhost even when the token is fetched
    # through 127.0.0.1 (the .NET resolver on Windows does not resolve *.localhost).
    [string]$TokenEndpoint = 'http://127.0.0.1:8081/realms/jungle/protocol/openid-connect/token',
    # Optional overrides; by default MessageGroupId = walletId and MessageDeduplicationId = SHA-256(providerId:messageId).
    [string]$GroupId,
    [string]$DeduplicationId
)

$ErrorActionPreference = 'Stop'
$envelope = Get-Content -LiteralPath $EnvelopePath -Raw | ConvertFrom-Json
$token = Invoke-RestMethod -Method Post -Uri $TokenEndpoint -ContentType 'application/x-www-form-urlencoded' -Body @{
    grant_type = 'client_credentials'
    client_id = $Provider
    client_secret = "local-$Provider-secret"
}
$request = @{
    provider = $Provider
    token = $token.access_token
    envelope = $envelope
}
if ($GroupId) { $request.groupId = $GroupId }
if ($DeduplicationId) { $request.deduplicationId = $DeduplicationId }
$request = $request | ConvertTo-Json -Depth 20 -Compress

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $request | docker compose run --rm --no-deps -T --entrypoint python provision /bootstrap/publish.py
    if ($LASTEXITCODE -ne 0) { throw 'SQS publication failed.' }
} finally {
    Pop-Location
}
