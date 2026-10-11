<#
.SYNOPSIS
  Live demonstration of the backend against the real Docker stack (Keycloak, PostgreSQL, MiniStack SQS/IAM, three app instances).

.DESCRIPTION
  Everything printed comes from real responses, SQL SELECTs and container logs. The script creates new wallets
  (identified by a "demo-<run>" prefix) and never touches existing data. Requirements: the stack is up
  (docker compose --profile e2e up -d --build --wait app app-2 app-3).

.PARAMETER Step
  Pause between phases.
#>
param(
    [switch]$Step,
    [string]$Api1 = 'http://localhost:8080',
    [string]$Api2 = 'http://localhost:8082',
    [string]$Api3 = 'http://localhost:8083',
    [string]$Keycloak = 'http://127.0.0.1:8081'
)

$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)
Add-Type -AssemblyName System.Net.Http
$script:http = New-Object System.Net.Http.HttpClient
$script:http.Timeout = [TimeSpan]::FromSeconds(40)
$script:results = New-Object System.Collections.Generic.List[object]
$script:tokens = @{}
$run = [guid]::NewGuid().ToString().Substring(0, 8)

# ---------------------------------------------------------------- output helpers
function Say($text, $color = 'Gray') { Write-Host $text -ForegroundColor $color }
function Phase($number, $title, $what) {
    if ($Step -and $number -gt 1) { Read-Host 'ENTER para continuar' | Out-Null }
    Write-Host ''
    Write-Host ('=' * 78) -ForegroundColor Cyan
    Write-Host ("FASE $number - $title") -ForegroundColor Cyan
    Write-Host ('=' * 78) -ForegroundColor Cyan
    Say $what 'Gray'
}
function Why($text) { Write-Host "  Por que importa: $text" -ForegroundColor DarkYellow }
function Check($name, [bool]$ok, $detail = '') {
    $script:results.Add([pscustomobject]@{ Name = $name; Ok = $ok; Detail = $detail })
    if ($ok) { Write-Host "[PASS] $name" -ForegroundColor Green } else { Write-Host "[FAIL] $name  $detail" -ForegroundColor Red }
    if ($detail -and $ok) { Write-Host "       $detail" -ForegroundColor DarkGray }
}

# ---------------------------------------------------------------- HTTP helpers
function Tok($client) {
    $entry = $script:tokens[$client]
    if ($entry -and ((Get-Date) - $entry.at).TotalSeconds -lt 200) { return $entry.value }
    $r = Invoke-RestMethod -Method Post -Uri "$Keycloak/realms/jungle/protocol/openid-connect/token" -Body @{
        grant_type = 'client_credentials'; client_id = $client; client_secret = "local-$client-secret" }
    $script:tokens[$client] = @{ value = $r.access_token; at = Get-Date }
    return $r.access_token
}
function New-Request($method, $url, $token, $body, $headers) {
    $req = New-Object System.Net.Http.HttpRequestMessage((New-Object System.Net.Http.HttpMethod $method), $url)
    if ($token) { $req.Headers.TryAddWithoutValidation('Authorization', "Bearer $token") | Out-Null }
    if ($headers) { foreach ($k in $headers.Keys) { $req.Headers.TryAddWithoutValidation($k, [string]$headers[$k]) | Out-Null } }
    if ($null -ne $body) {
        $req.Content = New-Object System.Net.Http.StringContent(($body | ConvertTo-Json -Depth 8 -Compress), [Text.Encoding]::UTF8, 'application/json')
    }
    return $req
}
function Read-Response($task) {
    $resp = $task.Result
    $raw = $resp.Content.ReadAsStringAsync().Result
    $json = $null
    if ($raw) { try { $json = $raw | ConvertFrom-Json } catch { } }
    $cid = ''
    $values = $null
    if ($resp.Headers.TryGetValues('X-Correlation-ID', [ref]$values)) { $cid = ($values | Select-Object -First 1) }
    return [pscustomobject]@{ Status = [int]$resp.StatusCode; Json = $json; Raw = $raw; Correlation = $cid }
}
function Api($method, $base, $path, $token = $null, $body = $null, $headers = $null) {
    try { return Read-Response ($script:http.SendAsync((New-Request $method "$base$path" $token $body $headers))) }
    catch { return [pscustomobject]@{ Status = 0; Json = $null; Raw = $_.Exception.GetBaseException().Message; Correlation = '' } }
}
function Money($amount) { @{ amount = $amount; currency = 'BRL' } }
function Wager-Command($wallet, $kind, $external, $amount, $reference = $null) {
    $c = [ordered]@{ providerId = 'provider-a'; externalTransactionId = $external; playerId = $wallet.playerId; walletId = $wallet.id
                     roundId = "demo-$run"; gameId = 'demo-game'; kind = $kind; money = (Money $amount) }
    if ($reference) { $c.referenceExternalTransactionId = $reference }
    return $c
}
function Send-Wager($base, $command, $key, $provider = 'provider-a', $correlation = $null) {
    $h = @{ 'Idempotency-Key' = $key }
    if ($correlation) { $h['X-Correlation-ID'] = $correlation }
    return Api 'POST' $base '/wagering/transactions' (Tok $provider) $command $h
}
function New-Wallet($amount) {
    $player = [guid]::NewGuid().ToString()
    $r = Api 'POST' $Api1 '/wallets' (Tok 'wallet-service') @{ playerId = $player; initialBalance = (Money $amount) }
    if ($r.Status -ne 201) { Say "criar carteira falhou: $($r.Status) $($r.Raw)" 'Red'; return $null }
    return [pscustomobject]@{ id = $r.Json.id; playerId = $player; version = $r.Json.version; balance = $r.Json.balance.amount; http = $r }
}
function Get-Wallet($w, $base = $Api1) { (Api 'GET' $base "/wallets/$($w.id)" (Tok 'wallet-service')).Json }
function Get-Ledger($w) { @((Api 'GET' $Api1 "/wallets/$($w.id)/ledger?limit=100" (Tok 'wallet-service')).Json.entries) }
function Reconcile($w, $base = $Api1) { (Api 'POST' $base "/wallets/$($w.id)/reconciliation" (Tok 'wallet-service')).Json }
function Psql($sql) { $o = docker compose exec -T postgres psql -U jungle -d jungle -At -F ' | ' -c $sql 2>&1; return (($o | Out-String).Trim()) }
function Container($service) { (docker compose --profile e2e ps -q $service).Trim() }
function Wait-Until($seconds, [scriptblock]$condition) {
    $end = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $end) { if (& $condition) { return $true }; Start-Sleep -Milliseconds 500 }
    return $false
}
function Show-Op($operation, $amount, $before, $after, $status, $version, $ledger) {
    Write-Host ''
    Write-Host '=====================================' -ForegroundColor Yellow
    Write-Host 'JUNGLE GAMING - LIVE WALLET DEMO' -ForegroundColor Yellow
    Write-Host '=====================================' -ForegroundColor Yellow
    Write-Host "OPERATION: $operation"
    Write-Host "AMOUNT: $amount BRL"
    Write-Host ''
    Write-Host "BALANCE BEFORE: $before BRL"
    Write-Host "BALANCE AFTER:  $after BRL"
    Write-Host ''
    Write-Host "TRANSACTION: $status"
    Write-Host "WALLET VERSION: $version"
    Write-Host "LEDGER: $ledger"
    Write-Host '=====================================' -ForegroundColor Yellow
}

# Executes one operation, prints the block with real values and verifies it against the expectation.
function Run-Op($wallet, $operation, $amount, $expectedBalance, $expectedStatus, $reference = $null, $label = $null) {
    $before = Get-Wallet $wallet
    $ledgerBefore = (Get-Ledger $wallet).Count
    $tag = if ($label) { $label } else { $operation.ToLower() }
    $external = "demo-$run-$tag"
    $command = Wager-Command $wallet $operation $external $amount $reference
    $key = "demo-${run}:$tag"
    $r = Send-Wager $Api1 $command $key
    $entries = Get-Ledger $wallet
    $ledgerNote = 'NO LEDGER ENTRY (no movement)'
    if ($entries.Count -gt $ledgerBefore) {
        $e = $entries[-1]
        $ledgerNote = "$($e.direction) RECORDED ($($e.balanceBefore.amount) -> $($e.balanceAfter.amount), ledger version $($e.walletVersion))"
    }
    $after = if ($r.Json.balance) { $r.Json.balance.amount } else { '?' }
    Show-Op $operation $amount $before.balance.amount $after $r.Json.status $r.Json.walletVersion $ledgerNote
    $ok = ($r.Status -eq 200) -and ($r.Json.status -eq $expectedStatus) -and ($after -eq $expectedBalance)
    Check "$operation $amount BRL -> $expectedBalance BRL" $ok "HTTP $($r.Status), status=$($r.Json.status), saldo=$after, versao=$($r.Json.walletVersion)"
    return [pscustomobject]@{ external = $external; key = $key; command = $command; response = $r; ledgerCount = $entries.Count; versionBefore = $before.version }
}

# ================================================================= FASE 1
Phase 1 'AMBIENTE (Docker Compose)' 'Mostra os servicos reais em execucao e consulta os health checks de cada instancia.'
docker compose --profile e2e ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'
Write-Host ''
Say 'URLs que voce pode abrir no navegador:' 'White'
Say "  API 1 (health)     $Api1/health/ready"
Say "  API 2 (health)     $Api2/health/ready"
Say "  API 3 (health)     $Api3/health/ready"
Say "  Keycloak (admin)   http://localhost:8081  (admin / local-admin-password, realm 'jungle')"
Say "  Broker SQS/IAM     http://localhost:4566/health"
$instances = @(@{ n = 'app'; u = $Api1 }, @{ n = 'app-2'; u = $Api2 }, @{ n = 'app-3'; u = $Api3 })
foreach ($i in $instances) {
    $live = Api 'GET' $i.u '/health/live'
    $ready = Api 'GET' $i.u '/health/ready'
    Check "Instancia $($i.n): live=$($live.Status) ready=$($ready.Status)" (($live.Status -eq 200) -and ($ready.Status -eq 200)) "readiness verifica PostgreSQL e as duas filas"
}
Why 'tres processos independentes compartilham PostgreSQL, SQS e Keycloak; nenhum guarda estado financeiro em memoria.'

# ================================================================= FASE 2
Phase 2 'AUTENTICACAO REAL (Keycloak, client_credentials)' 'Obtem tokens dos tres clientes locais e prova quem pode (e quem nao pode) chamar cada rota. Nenhum JWT e impresso.'
foreach ($c in 'wallet-service', 'provider-a', 'provider-b') {
    $t = Tok $c
    Check "Token do cliente '$c' emitido pelo Keycloak" ($t.Split('.').Count -eq 3) "JWT de $($t.Length) caracteres (nao exibido)"
}
$probe = Api 'GET' $Api1 "/wallets/$([guid]::NewGuid())"
Check 'Sem token -> HTTP 401' ($probe.Status -eq 401) "codigo=$($probe.Json.error.code)"
$probe = Api 'POST' $Api1 '/wallets' (Tok 'provider-a') @{ playerId = [guid]::NewGuid().ToString(); initialBalance = (Money '1.00') }
Check 'Provedor tentando abrir carteira (operacao interna) -> HTTP 403' ($probe.Status -eq 403) "codigo=$($probe.Json.error.code)"
$iso = New-Wallet '10.00'
$bCmd = [ordered]@{ providerId = 'provider-b'; externalTransactionId = "demo-$run-provider-b-bet"; playerId = $iso.playerId; walletId = $iso.id
                    roundId = "demo-$run"; gameId = 'demo-game'; kind = 'BET'; money = (Money '1.00') }
$bTx = Send-Wager $Api1 $bCmd "demo-${run}:provider-b-bet" 'provider-b'
Check 'Provider B opera com o proprio token (BET 1.00 -> PROCESSED)' (($bTx.Status -eq 200) -and ($bTx.Json.status -eq 'PROCESSED')) "transactionId=$($bTx.Json.transactionId)"
$own = Api 'GET' $Api1 "/wagering/transactions/$($bTx.Json.transactionId)" (Tok 'provider-b')
$cross1 = Api 'GET' $Api1 "/wagering/transactions/$($bTx.Json.transactionId)" (Tok 'provider-a')
$cross2 = Api 'GET' $Api1 "/providers/provider-b/wagering/transactions/demo-$run-provider-b-bet" (Tok 'provider-a')
$spoof = Send-Wager $Api1 $bCmd "demo-${run}:provider-b-spoof" 'provider-a'
Check 'Provider B le a propria transacao (200) e Provider A NAO a enxerga (404 por ID, 403 pelo caminho do provedor)' (($own.Status -eq 200) -and ($cross1.Status -eq 404) -and ($cross2.Status -eq 403)) "B=$($own.Status) A-por-id=$($cross1.Status) A-por-caminho=$($cross2.Status)"
Check 'Provider A nao consegue enviar uma operacao em nome do provider-b (corpo diz provider-b) -> 403' ($spoof.Status -eq 403) "HTTP $($spoof.Status) codigo=$($spoof.Json.error.code)"
Why 'o provedor e derivado do cliente autenticado (azp), nunca do corpo; so o servico interno mexe em carteiras.'

# ================================================================= FASE 3
Phase 3 'CARTEIRA E MOVIMENTOS (saldo, versao e ledger)' 'Cria uma carteira de 100.00 BRL e executa BET, WIN, LOSS, REFUND e ROLLBACK, conferindo saldo, versao e ledger a cada passo.'
$w1 = New-Wallet '100.00'
Say "POST /wallets  corpo: playerId=$($w1.playerId) initialBalance=100.00 BRL" 'Gray'
$opening = Get-Ledger $w1
Show-Op 'OPENING (criar carteira)' '100.00' '0.00' $w1.balance 'CREATED' $w1.version "$($opening[0].direction) RECORDED ($($opening[0].balanceBefore.amount) -> $($opening[0].balanceAfter.amount), ledger version $($opening[0].walletVersion))"
Check 'Carteira criada com 100.00 BRL, versao 1 e credito de abertura no ledger' (($w1.balance -eq '100.00') -and ($w1.version -eq 1) -and ($opening.Count -eq 1) -and ($opening[0].direction -eq 'CREDIT')) "walletId=$($w1.id)"
$bet = Run-Op $w1 'BET' '20.00' '80.00' 'PROCESSED'
Why 'a aposta debita dentro de UMA transacao SQL com lock da carteira; saldo, ledger, resultado e eventos nascem juntos.'
$win = Run-Op $w1 'WIN' '30.00' '110.00' 'PROCESSED' $bet.external
$loss = Run-Op $w1 'LOSS' '0.00' '110.00' 'PROCESSED'
Say '  LOSS nao move dinheiro: nao ha lancamento nem mudanca de versao (a versao so sobe quando o saldo muda). Mesmo assim e uma operacao concluida e gera o evento de processamento:' 'DarkYellow'
$lossEvents = Psql "SELECT string_agg(event_type, ', ') FROM outbox_events WHERE transaction_id = (SELECT id FROM wager_transactions WHERE external_transaction_id = 'demo-$run-loss')"
Say "  outbox da LOSS: $lossEvents" 'White'
Check 'LOSS: saldo e versao inalterados, evento de processamento sem evento de saldo' (($loss.response.Json.walletVersion -eq $win.response.Json.walletVersion) -and ($lossEvents -eq 'WagerTransactionProcessed')) "versao $($loss.response.Json.walletVersion)"
$refund = Run-Op $w1 'REFUND' '20.00' '130.00' 'PROCESSED' $bet.external
Say '  O REFUND referencia a BET original pelo ID externo; o sistema a localizou e devolveu exatamente 20.00.' 'DarkYellow'
$rollback = Run-Op $w1 'ROLLBACK' '20.00' '110.00' 'PROCESSED' $refund.external
$final1 = Get-Ledger $w1
Say '  Ledger completo (append-only; o ROLLBACK e um novo lancamento compensatorio, nada foi editado):' 'White'
$final1 | ForEach-Object { Say ("    v{0}  {1,-6} {2,7}  {3,7} -> {4,7}" -f $_.walletVersion, $_.direction, $_.money.amount, $_.balanceBefore.amount, $_.balanceAfter.amount) 'Gray' }
$updates = Psql "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = '$($w1.id)'"
Check 'Ledger append-only com 5 lancamentos (abertura, BET, WIN, REFUND, ROLLBACK)' (($final1.Count -eq 5) -and ($updates -eq '5')) "entradas no PostgreSQL: $updates"
Why 'saldo = soma do ledger; correcoes sao novos lancamentos, e a role da aplicacao nem tem UPDATE/DELETE nessa tabela.'

# ================================================================= FASE 4
Phase 4 'IDEMPOTENCIA' 'Reenvia a BET original com a mesma chave e depois uma carga diferente com a mesma chave.'
$balBefore = (Get-Wallet $w1).balance.amount
$ledgerBefore = (Get-Ledger $w1).Count
$replay = Send-Wager $Api1 $bet.command $bet.key
Say "REPLAY  HTTP $($replay.Status)  transactionId=$($replay.Json.transactionId)  idempotentReplay=$($replay.Json.idempotentReplay)" 'White'
$balAfter = (Get-Wallet $w1).balance.amount
$ledgerAfter = (Get-Ledger $w1).Count
Check 'Replay devolve a mesma transacao e o resultado financeiro ORIGINAL (saldo da epoca: 80.00)' (($replay.Json.transactionId -eq $bet.response.Json.transactionId) -and ($replay.Json.idempotentReplay -eq $true) -and ($replay.Json.balance.amount -eq '80.00')) "saldo no replay=$($replay.Json.balance.amount); saldo atual da carteira=$balAfter"
Check 'Replay nao criou lancamento nem alterou o saldo atual' (($ledgerAfter -eq $ledgerBefore) -and ($balAfter -eq $balBefore)) "ledger $ledgerBefore -> $ledgerAfter; saldo $balBefore -> $balAfter"
$different = Wager-Command $w1 'BET' $bet.external '25.00'
$conflict = Send-Wager $Api1 $different $bet.key
Check 'Mesma chave com payload diferente -> HTTP 409' (($conflict.Status -eq 409) -and ($conflict.Json.error.code -eq 'IDEMPOTENCY_CONFLICT')) "HTTP $($conflict.Status) codigo=$($conflict.Json.error.code)"
$betRows = Psql "SELECT count(*) FROM wager_transactions WHERE wallet_id = '$($w1.id)' AND external_transaction_id = '$($bet.external)'"
$debits = Psql "SELECT count(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.wallet_id = '$($w1.id)' AND t.external_transaction_id = '$($bet.external)'"
Say "  PostgreSQL (somente leitura): transacoes dessa BET = $betRows | lancamentos dessa BET = $debits" 'White'
Check 'No PostgreSQL existe 1 transacao e 1 lancamento para a BET, apos varios envios' (($betRows -eq '1') -and ($debits -eq '1'))
Say '  "Mesmo que a requisicao seja recebida varias vezes, o dinheiro so pode ser movimentado uma vez."' 'Yellow'
Why 'a identidade da operacao e persistente (constraint no banco), entao sobrevive a retries, reinicios e outras instancias.'

# ================================================================= FASE 5
Phase 5 'CONCORRENCIA (duas apostas de 80.00 sobre 100.00, em instancias diferentes)' 'Dispara as duas requisicoes ao mesmo tempo, uma na porta 8080 e outra na 8082.'
$w2 = New-Wallet '100.00'
$cmdA = Wager-Command $w2 'BET' "demo-$run-race-a" '80.00'
$cmdB = Wager-Command $w2 'BET' "demo-$run-race-b" '80.00'
$tok = Tok 'provider-a'
$reqA = New-Request 'POST' "$Api1/wagering/transactions" $tok $cmdA @{ 'Idempotency-Key' = "demo-${run}:race-a" }
$reqB = New-Request 'POST' "$Api2/wagering/transactions" $tok $cmdB @{ 'Idempotency-Key' = "demo-${run}:race-b" }
$taskA = $script:http.SendAsync($reqA)
$taskB = $script:http.SendAsync($reqB)
[System.Threading.Tasks.Task]::WaitAll(@($taskA, $taskB))
$ra = Read-Response $taskA
$rb = Read-Response $taskB
function Outcome($r) { if ($r.Json.status -eq 'REJECTED') { "REJECTED ($($r.Json.failureCode))" } else { $r.Json.status } }
$w2now = Get-Wallet $w2
$betDebits = Psql "SELECT count(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.wallet_id = '$($w2.id)' AND t.kind = 'BET'"
$rec2 = Reconcile $w2
Write-Host ''
Write-Host 'CONCURRENCY DEMONSTRATION' -ForegroundColor Yellow
Write-Host "INITIAL BALANCE: 100.00 BRL"
Write-Host "BET A: 80.00 BRL (porta 8080) -> $(Outcome $ra)"
Write-Host "BET B: 80.00 BRL (porta 8082) -> $(Outcome $rb)"
Write-Host "FINAL BALANCE: $($w2now.balance.amount) BRL"
Write-Host "DUPLICATE DEBITS: $([int]$betDebits - 1) (lancamentos de aposta no ledger: $betDebits)"
Write-Host "RECONCILIATION: $(if ($rec2.consistent) { 'CONSISTENT' } else { 'DIVERGENT' })"
$statuses = @($ra.Json.status, $rb.Json.status) | Sort-Object
$raceOk = ($statuses -join ',') -eq 'PROCESSED,REJECTED' -and ($w2now.balance.amount -eq '20.00') -and ($betDebits -eq '1') -and $rec2.consistent
Check 'Exatamente uma aposta processada, a outra rejeitada por saldo insuficiente, saldo 20.00 e um unico debito' $raceOk "A=$(Outcome $ra) B=$(Outcome $rb)"
if ($raceOk) {
    $ra2 = Send-Wager $Api3 $cmdA "demo-${run}:race-a"
    $rb2 = Send-Wager $Api3 $cmdB "demo-${run}:race-b"
    Check 'Reenvio das duas chaves (na instancia 3) devolve os resultados persistidos' (($ra2.Json.idempotentReplay -eq $true) -and ($rb2.Json.idempotentReplay -eq $true) -and ($ra2.Json.status -eq $ra.Json.status) -and ($rb2.Json.status -eq $rb.Json.status)) "A=$($ra2.Json.status) B=$($rb2.Json.status)"
} else { Say 'Resultado divergente: dados preservados em ' 'Red'; Say "  walletId=$($w2.id)" 'Red' }
Why 'o lock FOR UPDATE da carteira serializa as decisoes ENTRE processos; a segunda ve o saldo ja debitado. Nao e mutex em memoria.'

# ================================================================= FASE 6
Phase 6 'SQS NA PRATICA (entrada assincrona + inbox + outbox + saida)' 'Publica uma aposta de 5.00 na fila de entrada com a identidade IAM do provedor e o JWT dele; depois segue a mensagem ate o evento na fila de saida.'
$sqsExternal = "demo-$run-sqs-bet"
$messageId = [guid]::NewGuid().ToString()
$envelope = [ordered]@{ messageId = $messageId; type = 'WagerTransactionRequested'; occurredAt = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    data = [ordered]@{ providerId = 'provider-a'; externalTransactionId = $sqsExternal; idempotencyKey = "demo-${run}:sqs-bet"; playerId = $w2.playerId; walletId = $w2.id
                       roundId = "demo-$run"; gameId = 'demo-game'; kind = 'BET'; money = (Money '5.00') } }
$envFile = Join-Path ([IO.Path]::GetTempPath()) "demo-$run-envelope.json"
$envelope | ConvertTo-Json -Depth 6 | Out-File -FilePath $envFile -Encoding ascii
$identity = Get-Content '.local/provider-a.json' -Raw | ConvertFrom-Json
Say "1) Produtor (IAM do broker): $($identity.UserName)  ARN=$($identity.Arn)  (so pode SendMessage na fila de entrada; segredo nao exibido)" 'White'
Say "   Fila FIFO: wager-transactions.fifo  MessageGroupId=walletId  atributo Authorization = Bearer <JWT do provider-a>" 'White'
$balanceBeforeSqs = (Get-Wallet $w2).balance.amount
$publish = & "$PSScriptRoot/publish-sqs.ps1" -EnvelopePath $envFile -Provider 'provider-a'
Say "2) Mensagem publicada na fila: $publish" 'White'
Check 'Mensagem aceita pelo broker' ($publish -match 'MessageId') $publish
$processed = Wait-Until 30 { (Psql "SELECT status FROM wager_transactions WHERE external_transaction_id = '$sqsExternal'") -eq 'PROCESSED' }
Check 'Consumidor processou a aposta (status PROCESSED no PostgreSQL)' $processed
$txRow = Psql "SELECT id, kind, status, amount_minor FROM wager_transactions WHERE external_transaction_id = '$sqsExternal'"
Say "3) Transacao no PostgreSQL: $txRow" 'White'
$txId = (Psql "SELECT id FROM wager_transactions WHERE external_transaction_id = '$sqsExternal'")
$balanceAfterSqs = (Get-Wallet $w2).balance.amount
Say "4) Saldo: $balanceBeforeSqs -> $balanceAfterSqs BRL" 'White'
Check 'Saldo atualizado pelo caminho assincrono (20.00 -> 15.00)' (($balanceBeforeSqs -eq '20.00') -and ($balanceAfterSqs -eq '15.00'))
$inbox = Psql "SELECT message_id, sender_id, disposition, completed_at IS NOT NULL AS completed FROM inbox_messages WHERE message_id = '$messageId'"
Say "5) Inbox (message_id | emissor autenticado pelo JWT | disposicao | concluida): $inbox" 'White'
Check 'Inbox concluida com ACK e emissor = client id validado pelo JWT' ($inbox -match '\| provider-a \| ACK \| t') $inbox
$published = Wait-Until 20 { (Psql "SELECT count(*) FROM outbox_events WHERE transaction_id = '$txId' AND published_at IS NOT NULL") -eq '2' }
$outbox = Psql "SELECT event_type, published_at IS NOT NULL AS published FROM outbox_events WHERE transaction_id = '$txId' ORDER BY event_type"
Say "6) Outbox (evento | publicado): $($outbox -replace "`n", '  ||  ')" 'White'
Check 'Dois eventos gravados na MESMA transacao do saldo e ja publicados' $published
Say '7) Consumidor downstream (identidade events-consumer) lendo wallet-events.fifo:' 'White'
$eventLines = docker compose run --rm --no-deps -T -v "${PWD}/scripts:/demo:ro" --entrypoint python provision /demo/demo_consume_events.py $w2.id $txId 2>$null
$mine = @($eventLines | Where-Object { $_ -match '"mine": true' })
$eventLines | ForEach-Object { Say "   $_" 'Gray' }
Check 'Eventos da transacao chegaram na fila de saida (Processed + BalanceChanged)' ($mine.Count -ge 2) "$($mine.Count) eventos"
$handled = (@('app', 'app-2', 'app-3') | ForEach-Object { docker logs --since 5m (Container $_) 2>&1 | Select-String -SimpleMatch $messageId } | Select-Object -First 1)
if ($handled) { Say "8) Log estruturado do consumidor: $($handled.Line)" 'DarkGray' }
Say '   Tres momentos distintos: (a) mensagem RECEBIDA, (b) operacao CONFIRMADA no banco (commit unico com inbox ACK), (c) evento PUBLICADO. Um crash entre eles e recuperavel.' 'DarkYellow'
Why 'a entrega e at-least-once; a duplicata e absorvida por inbox + idempotencia persistente. Nao se promete exactly-once no transporte.'
Remove-Item $envFile -ErrorAction SilentlyContinue

# ================================================================= FASE 7
Phase 7 'TRES INSTANCIAS, UMA UNICA FONTE DE VERDADE' 'Cria uma operacao na instancia 1, le o resultado pela 2 e reenvia a mesma operacao pela 3.'
$w3 = New-Wallet '100.00'
$multi = Wager-Command $w3 'BET' "demo-$run-multi" '10.00'
$m1 = Send-Wager $Api1 $multi "demo-${run}:multi"
Say "Instancia 1 (8080): BET 10.00 -> $($m1.Json.status), saldo $($m1.Json.balance.amount)" 'White'
$via2 = Get-Wallet $w3 $Api2
$byExternal = Api 'GET' $Api2 "/providers/provider-a/wagering/transactions/demo-$run-multi" (Tok 'provider-a')
Say "Instancia 2 (8082): saldo da carteira $($via2.balance.amount) | consulta da transacao -> $($byExternal.Json.status)" 'White'
$m3 = Send-Wager $Api3 $multi "demo-${run}:multi"
Say "Instancia 3 (8083): reenvio -> idempotentReplay=$($m3.Json.idempotentReplay), mesma transacao=$($m3.Json.transactionId -eq $m1.Json.transactionId)" 'White'
$bals = foreach ($i in $instances) { (Get-Wallet $w3 $i.u).balance.amount }
$ledger3 = (Get-Ledger $w3).Count
Check 'As tres instancias leem o mesmo saldo (90.00) e o replay nao gerou novo lancamento' ((($bals | Select-Object -Unique) -eq '90.00') -and ($m3.Json.idempotentReplay -eq $true) -and ($ledger3 -eq 2)) "saldos: $($bals -join ' / '); lancamentos: $ledger3"
Why 'o estado financeiro vive so no PostgreSQL; escalar e adicionar processos.'

# ================================================================= FASE 8
Phase 8 'RECONCILIACAO E OBSERVABILIDADE' 'Compara o saldo armazenado com a soma exata do ledger de cada carteira e mostra health, metricas e logs correlacionados.'
foreach ($pair in @(@('Carteira 1 (movimentos)', $w1), @('Carteira 2 (concorrencia + SQS)', $w2), @('Carteira 3 (tres instancias)', $w3))) {
    $rec = Reconcile $pair[1] $Api3
    Write-Host ''
    Write-Host "FINANCIAL RECONCILIATION - $($pair[0])" -ForegroundColor Yellow
    Write-Host ("Stored balance:      {0,8} BRL" -f $rec.storedBalance.amount)
    Write-Host ("Calculated balance:  {0,8} BRL" -f $rec.calculatedBalance.amount)
    Write-Host ("Difference:          {0,8} BRL" -f $rec.difference.amount)
    Write-Host "Ledger entries:      $($rec.checkedEntries)"
    Write-Host "Consistent:          $($rec.consistent)"
    Write-Host "RESULT: $(if ($rec.consistent) { 'PASSED' } else { 'FAILED' })"
    Check "Reconciliacao: $($pair[0])" ([bool]$rec.consistent -and ($rec.difference.amount -eq '0.00')) "armazenado=$($rec.storedBalance.amount) calculado=$($rec.calculatedBalance.amount)"
}
Why 'a reconciliacao so reporta; nunca corrige sozinha. Divergencia seria incidente, nao ajuste automatico.'
Write-Host ''
Say 'Metricas (/metrics exige o cliente interno), por instancia:' 'White'
foreach ($i in $instances) {
    $m = (Api 'GET' $i.u '/metrics' (Tok 'wallet-service')).Raw -split "`n"
    $pick = $m | Where-Object { $_ -match '^(wager_transactions_total\{.*status="(PROCESSED|REJECTED)".*\} [1-9]|idempotent_replays_total |outbox_pending_events |wager_processing_duration_seconds_count\{transport=)' }
    Say "  [$($i.n)]" 'Cyan'
    $pick | ForEach-Object { Say "    $_" 'Gray' }
}
$correlation = "demo-$run-corr"
$tracked = Send-Wager $Api1 (Wager-Command $w3 'BET' "demo-$run-corr" '1.00') "demo-${run}:corr" 'provider-a' $correlation
Start-Sleep -Seconds 3
Say "Logs correlacionados da operacao (X-Correlation-ID=$correlation):" 'White'
$logLine = (@('app', 'app-2', 'app-3') | ForEach-Object { docker logs --since 2m (Container $_) 2>&1 | Select-String -SimpleMatch $correlation } | Select-Object -First 1)
if ($logLine) { Say "  log: $($logLine.Line)" 'Gray' }
$evCorr = Psql "SELECT DISTINCT payload->>'correlationId' FROM outbox_events WHERE transaction_id = (SELECT id FROM wager_transactions WHERE external_transaction_id = 'demo-$run-corr')"
Say "  outbox: correlationId dos eventos = $evCorr" 'Gray'
Check 'Mesmo correlationId na resposta HTTP, no log estruturado e nos eventos' (($tracked.Correlation -eq $correlation) -and ($null -ne $logLine) -and ($evCorr -eq $correlation))

# ================================================================= FASE 9
Phase 9 'RECUPERACAO APOS FALHA (uma instancia)' 'Derruba so a instancia 2 (docker stop -t 0, equivalente a SIGKILL), confere as outras, e a reinicia.'
$balanceW1 = (Get-Wallet $w1).balance.amount
$app2 = Container 'app-2'   # guardado antes: `docker compose ps -q` nao lista containers parados
docker stop -t 0 $app2 | Out-Null
Start-Sleep -Seconds 2
$down = Api 'GET' $Api2 '/health/live'
$up1 = Api 'GET' $Api1 '/health/ready'
$up3 = Api 'GET' $Api3 '/health/ready'
Check 'Instancia 2 indisponivel; instancias 1 e 3 continuam respondendo' (($down.Status -eq 0) -and ($up1.Status -eq 200) -and ($up3.Status -eq 200)) "app-2=$($down.Status) (conexao recusada) app=$($up1.Status) app-3=$($up3.Status)"
$during = Send-Wager $Api3 $bet.command $bet.key
Check 'Com a instancia 2 fora, o saldo segue no PostgreSQL e o replay continua seguro (via instancia 3)' (($during.Json.idempotentReplay -eq $true) -and ((Get-Wallet $w1 $Api1).balance.amount -eq $balanceW1)) "saldo=$balanceW1"
docker start $app2 | Out-Null
$back = Wait-Until 90 { (Api 'GET' $Api2 '/health/ready').Status -eq 200 }
Check 'Instancia 2 reiniciada e pronta' $back
$after = Send-Wager $Api2 $bet.command $bet.key
$balAfterRestart = (Get-Wallet $w1 $Api2).balance.amount
Check 'A instancia reiniciada le o mesmo estado e trata o replay sem novo efeito' (($after.Json.idempotentReplay -eq $true) -and ($balAfterRestart -eq $balanceW1)) "saldo lido pela instancia 2: $balAfterRestart"
Why 'nada vive no processo: matar uma instancia nao perde dinheiro nem estado; so capacidade.'

# ================================================================= RESUMO
Write-Host ''
Write-Host ('=' * 78) -ForegroundColor Cyan
Write-Host 'RESUMO' -ForegroundColor Cyan
Write-Host ('=' * 78) -ForegroundColor Cyan
foreach ($r in $script:results) { if ($r.Ok) { Write-Host "[PASS] $($r.Name)" -ForegroundColor Green } else { Write-Host "[FAIL] $($r.Name)  $($r.Detail)" -ForegroundColor Red } }
$failed = @($script:results | Where-Object { -not $_.Ok }).Count
Write-Host ''
Write-Host "Carteiras da demonstracao (prefixo demo-$run):" -ForegroundColor White
Write-Host "  carteira 1 (movimentos)      $($w1.id)  saldo final $((Get-Wallet $w1).balance.amount)"
Write-Host "  carteira 2 (concorrencia/SQS) $($w2.id)  saldo final $((Get-Wallet $w2).balance.amount)"
Write-Host "  carteira 3 (instancias)      $($w3.id)  saldo final $((Get-Wallet $w3).balance.amount)"
Write-Host ''
Write-Host "Verificacoes: $($script:results.Count - $failed) PASS, $failed FAIL" -ForegroundColor $(if ($failed) { 'Red' } else { 'Green' })
Write-Host 'Os containers e os dados da demonstracao foram mantidos.' -ForegroundColor Gray
if ($failed) { exit 1 }
