# Jungle Gaming — processamento de apostas em Go

Backend que processa operações de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) por **HTTP e SQS**, com saldo exato, ledger auditável, idempotência persistente e recuperação após falhas entre processos.

O projeto parte do [desafio oficial da Jungle Gaming](https://github.com/junglegaming/backend-challenge-go). Interpretações e decisões próprias estão em [ARCHITECTURE.md](ARCHITECTURE.md); requisitos, riscos e a **matriz de evidências** (requisito → teste que o comprova) estão em [docs/CHALLENGE_ANALYSIS.md](docs/CHALLENGE_ANALYSIS.md); o histórico de execução, com resultados reais, está em [docs/IMPLEMENTATION_PROGRESS.md](docs/IMPLEMENTATION_PROGRESS.md).

Código, nomes, comentários, logs e mensagens da API estão em inglês; a documentação explicativa, em português.

## O que existe

Um monólito modular em Go, replicável em vários processos independentes:

- `net/http` + Uber Fx: um `fx.Module` por responsabilidade (banco, broker, identidade, cada worker, HTTP) com *hooks* de início/parada que liberam o que o módulo adquiriu — [cmd/server](cmd/server/modules.go).
- PostgreSQL 18 + `pgx`: SQL explícito, uma operação financeira por transação, `SELECT … FOR UPDATE` por carteira, constraints e *triggers* que recusam lançamentos inconsistentes até quando é a própria aplicação que pede — [migrations/](migrations/000001_init.up.sql).
- `Money` em `int64` (centavos) com moeda explícita, JSON decimal em string, detecção de overflow; nenhum `float` em valores monetários — [internal/domain](internal/domain).
- Keycloak (OIDC): JWT validado por assinatura (RS256), *issuer*, *audience*, validade e mapeamento `azp` → provedor — [internal/auth](internal/auth/auth.go).
- SQS FIFO (MiniStack, que **aplica** as políticas IAM por papel): consumidor com **inbox** persistente, DLQ por *redrive* do broker, **transactional outbox** com *lease* e publisher concorrente, worker de referências pendentes com *backoff* — [internal/messaging](internal/messaging), [internal/workers](internal/workers).

HTTP e SQS chamam o **mesmo caso de uso** ([internal/application](internal/application/service.go)); a idempotência vive no PostgreSQL e sobrevive a reinícios de todos os processos. Um *replay* devolve o resultado original (inclusive o saldo da época), mesmo que a carteira já tenha se movimentado.

### Operações

| Operação | Comportamento |
| --- | --- |
| `BET` | Debita valor positivo se houver saldo; senão, rejeição persistida `INSUFFICIENT_FUNDS` |
| `WIN` | Credita valor positivo; pode referenciar uma aposta |
| `LOSS` | Registra conclusão com valor `"0.00"`, sem alterar saldo |
| `REFUND` | Devolve integralmente uma aposta processada |
| `ROLLBACK` | Inverte integralmente uma aposta, prêmio ou refund processado (nunca deixa saldo negativo) |
| `OPENING` | Crédito interno de abertura da carteira; não é aceito como operação externa |

### Estados de uma transação

| Estado | Significado | Quem produz |
| --- | --- | --- |
| `PENDING` | Aceita e ainda sem resultado; existe só dentro da transação do banco (nunca é observável) | `Process` |
| `PENDING_REFERENCE` | Reversão cuja referência ainda não existe ou ainda não foi concluída | `Evaluate` |
| `PROCESSED` | Efeito financeiro aplicado (ou `LOSS` registrada) | `Evaluate` |
| `REJECTED` | Rejeição de negócio definitiva e persistida (`failureCode`) | `Evaluate` |
| `FAILED` | **Falha permanente de infraestrutura**, persistida para auditoria; não move dinheiro e não emite evento de integração | `Evaluate` |

As transições são validadas pelo agregado [`WagerTransaction`](internal/domain/transaction.go) antes de qualquer escrita: estados terminais são imutáveis, `FAILED`/`REJECTED` exigem código e `PROCESSED` exige saldo e versão.

`FAILED` tem **um** produtor legítimo: a carteira não consegue mais avançar o contador de versão (`WALLET_VERSION_EXHAUSTED`, `int64` esgotado). É um fato determinístico — nenhuma nova tentativa pode funcionar — e só vale para decisões que exigem movimento (uma `LOSS`, ou uma `BET` que já seria `INSUFFICIENT_FUNDS`, continuam funcionando). **Erros de banco, timeouts e falhas do publisher nunca viram `FAILED`**: não provam nada sobre a transação e uma nova tentativa com a mesma chave é segura. HTTP responde `500` com `status: "FAILED"`; uma entrega SQS é registrada na inbox com disposição `DLQ`, não é reconhecida e o *redrive* do broker a leva à DLQ em segundos.

Valores exigem exatamente duas casas (`{"amount":"20.00","currency":"BRL"}`); os endpoints aceitam BRL. Uma referência só recebe **uma** reversão bem-sucedida, somando `REFUND` e `ROLLBACK`. Uma reversão que chega antes da aposta fica `PENDING_REFERENCE` (HTTP `202`) e é concluída por um worker em qualquer instância; se a referência não aparecer em 24 h, vira rejeição definitiva `REFERENCE_NOT_FOUND`.

## Pré-requisitos

- Docker Desktop (ou Engine) com Compose v2 e containers Linux. **Reserve ≥ 4 GB de RAM para a VM do Docker**: o Keycloak é pesado e, em 3,5 GB, o primeiro boot levou ~15 minutos (os seguintes são bem mais rápidos).
- PowerShell para os exemplos abaixo.
- Go 1.27.2 no host somente para rodar os testes de integração/E2E fora de container (os unitários com `-race` rodam no container `tests`).
- A porta `5432` do host costuma estar ocupada por um PostgreSQL local; por isso o Compose publica o banco em **`15432`** por padrão (`POSTGRES_PORT`). O `.env` é **opcional**: sem ele, `docker compose up --build` já usa esses padrões.
- Em Windows, o .NET do PowerShell **não resolve `*.localhost`**: os exemplos obtêm o token em `127.0.0.1:8081`. O Keycloak tem o *hostname* fixado em `idp.localhost`, então o claim `iss` é o mesmo (os containers usam `idp.localhost` por alias de rede).

## Subir o ambiente

```powershell
Copy-Item .env.example .env
docker compose up -d --build --wait app
```

Isso sobe PostgreSQL, Keycloak (realm, clientes e roles importados de [deploy/keycloak](deploy/keycloak/jungle-realm.json)), o broker SQS/IAM (`broker`, MiniStack), aplica as migrations (serviço `migrate`, execução única) e provisiona filas, identidades e políticas (serviço `provision`, idempotente, imagem própria em [deploy/broker](deploy/broker)), e só então inicia a aplicação em `http://localhost:8080`. Credenciais AWS locais por papel são gravadas em `.local/` (fora do git). O comando literal da entrega, `docker compose up --build`, faz o mesmo em primeiro plano.

Três instâncias independentes (mesma imagem, conexões e memória próprias) para cenários distribuídos:

```powershell
docker compose --profile e2e up -d --build --wait app app-2 app-3   # portas 8080, 8082, 8083
```

`app-2` e `app-3` reutilizam a imagem construída por `app`; suba-os sempre junto de `app` com `--build`.

Rotinas úteis:

```powershell
docker compose run --rm provision          # recria filas/identidades (idempotente; saída 0)
docker compose run --rm migrate            # aplica migrations pendentes ("no change" se já aplicadas)
docker compose logs -f app                 # logs JSON estruturados

# Reverter UMA migration em banco local descartável (apaga dados): pare os escritores antes.
docker compose stop app app-2 app-3
docker compose run --rm migrate -path /migrations -database 'postgres://jungle:localpassword@postgres:5432/jungle?sslmode=disable' down 1
```

Depois de reiniciar o Docker (ou só o broker), as filas e identidades em memória do emulador somem: rode `docker compose run --rm provision` de novo antes de iniciar a aplicação.

## Configuração

Valores locais descartáveis em [.env.example](.env.example); nunca reutilize estas credenciais fora do ambiente local.

| Variável | Padrão / uso |
| --- | --- |
| `HTTP_ADDR` | `:8080` |
| `DATABASE_URL` | Role de runtime `jungle_app` (sem `DELETE`/`TRUNCATE`; sem `UPDATE` no ledger) |
| `OIDC_ISSUER_URL`, `OIDC_AUDIENCE` | `http://idp.localhost:8081/realms/jungle`, `jungle-backend` |
| `PROVIDER_CLIENT_MAP` | JSON `{"client-id":"provider-id"}`; padrão `provider-a`, `provider-b` |
| `INTERNAL_CLIENT_ID` | `wallet-service` (abre/consulta carteiras, ledger, reconciliação) |
| `AWS_REGION`, `SQS_ENDPOINT_URL` | `us-east-1`; o broker (`http://broker:4566`) |
| `SQS_INPUT_QUEUE_URL`, `SQS_OUTPUT_QUEUE_URL` | `wager-transactions.fifo`, `wallet-events.fifo` |
| `SQS_INPUT_DLQ_URL` | Opcional; habilita a métrica `sqs_dlq_messages` |
| `PROCESSING_TIMEOUT`, `SHUTDOWN_TIMEOUT`, `REFERENCE_TTL` | `10s`, `30s`, `24h` |

Constantes no código/provisionamento (não são variáveis): lease de 30 s para outbox e referências, *backoff* de 1 s a 5 min com jitter, visibilidade SQS de 60 s, `maxReceiveCount` 5, retenção da DLQ de 14 dias.

## Chamadas HTTP

Todas as rotas de negócio exigem Bearer token. `provider-a`/`provider-b` só enxergam as próprias transações; `wallet-service` abre e consulta carteiras. `/health/live` e `/health/ready` são públicos; `/metrics` exige o cliente interno.

```powershell
$api = 'http://localhost:8080'
$tokenEndpoint = 'http://127.0.0.1:8081/realms/jungle/protocol/openid-connect/token'
function Get-Token($client) {
    (Invoke-RestMethod -Method Post -Uri $tokenEndpoint -Body @{
        grant_type = 'client_credentials'; client_id = $client; client_secret = "local-$client-secret" }).access_token
}
$internal = @{ Authorization = "Bearer $(Get-Token 'wallet-service')" }
$provider = @{ Authorization = "Bearer $(Get-Token 'provider-a')"; 'Idempotency-Key' = 'provider-a:bet-001' }

$playerId = [guid]::NewGuid().ToString()
$wallet = Invoke-RestMethod -Method Post -Uri "$api/wallets" -Headers $internal -ContentType 'application/json' -Body (@{
    playerId = $playerId; initialBalance = @{ amount = '100.00'; currency = 'BRL' } } | ConvertTo-Json -Depth 5)

$bet = Invoke-RestMethod -Method Post -Uri "$api/wagering/transactions" -Headers $provider -ContentType 'application/json' -Body (@{
    providerId = 'provider-a'; externalTransactionId = 'bet-001'; playerId = $playerId; walletId = $wallet.id
    roundId = 'round-001'; gameId = 'game-001'; kind = 'BET'
    money = @{ amount = '20.00'; currency = 'BRL' } } | ConvertTo-Json -Depth 5)
# status PROCESSED, balance 80.00, walletVersion 2. Repetir a mesma chamada: mesmo transactionId e "idempotentReplay": true.
# Mesma chave com valor diferente: 409 IDEMPOTENCY_CONFLICT.

Invoke-RestMethod -Uri "$api/providers/provider-a/wagering/transactions/bet-001" -Headers $provider
Invoke-RestMethod -Uri "$api/wallets/$($wallet.id)" -Headers $internal
Invoke-RestMethod -Uri "$api/wallets/$($wallet.id)/ledger?limit=50" -Headers $internal            # cursor opaco em nextCursor
Invoke-RestMethod -Method Post -Uri "$api/wallets/$($wallet.id)/reconciliation" -Headers $internal  # consistent: true
```

O envio usa `Idempotency-Key` (1–128 caracteres) **somente no header**. `X-Correlation-ID` (`[A-Za-z0-9._:-]{1,128}`) é devolvido na resposta e propagado aos eventos; valores fora desse formato são substituídos por um UUID.

| Resposta ao envio | Interpretação |
| --- | --- |
| `200` | Processada ou replay de sucesso |
| `202` | Referência pendente persistida; acompanhar a URL em `Location` |
| `400` | Entrada inválida, sem efeito financeiro |
| `401` / `403` | Autenticação / autorização |
| `404` | Recurso inexistente ou não visível ao principal |
| `409` | Conflito de chave ou de ID externo |
| `422` | Rejeição de negócio persistida e definitiva (`failureCode`) |
| `500` com `FAILED` | Falha permanente persistida e auditável (`failureCode` `WALLET_VERSION_EXHAUSTED`); repetir devolve o mesmo resultado |
| `503` | Indisponibilidade transitória (`Retry-After`); repetir com a **mesma** chave |

Erros usam `{"error":{"code","message","retryable"},"correlationId"}`. Abrir de novo a carteira do mesmo jogador/moeda retorna `409 WALLET_ALREADY_EXISTS` (sem segundo crédito).

## Entrada SQS

O produtor publica na fila FIFO `wager-transactions.fifo` com:

- corpo: o envelope abaixo (JSON estrito: campos desconhecidos, chaves duplicadas, Unicode inválido e valores monetários em número são recusados; `messageId` segue `[A-Za-z0-9._:-]{1,128}`);
- `MessageGroupId = walletId` (validado contra o corpo) e `MessageDeduplicationId = SHA-256(providerId + ":" + messageId)`;
- atributo de mensagem `Authorization = "Bearer <JWT do provedor>"`.

```json
{
  "messageId": "ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-10-09T12:00:00Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "bet-002",
    "idempotencyKey": "provider-a:bet-002",
    "playerId": "b734f231-163d-4bed-8d91-23d81a31821e",
    "walletId": "05f3842e-55de-4e81-85a9-b547a85b0ef8",
    "roundId": "round-002",
    "gameId": "game-001",
    "kind": "BET",
    "money": { "amount": "10.00", "currency": "BRL" }
  }
}
```

Publicar um envelope salvo em arquivo (obtém o token no Keycloak e envia com as credenciais locais do provedor):

```powershell
./scripts/publish-sqs.ps1 -EnvelopePath .\envelope.json -Provider provider-a
```

| Resultado do tratamento | Destino da mensagem |
| --- | --- |
| Processada, rejeição de negócio ou referência pendente (tudo persistido) | Inbox `ACK` no mesmo commit; depois `DeleteMessage` |
| Redelivery de mensagem já concluída | Reconhecida sem segundo efeito |
| Falha transitória (banco, IdP) | Mantida; visibilidade ajustada para 5/10/20/40 s |
| Envelope, token (ausente, inválido, expirado) ou grupo inválidos; conflito de hash/identidade; `FAILED` | Não removida: visibilidade reduzida a 2 s e o *redrive* do broker a leva à DLQ após 5 recebimentos (≈ 10–15 s) |

Atenção operacional: numa FIFO, uma mensagem "veneno" retém **o resto do seu grupo (a carteira)** até ser removida ou ir à DLQ. Como o consumidor reduz a visibilidade de falhas permanentes a 2 s, isso dura ≈ 10–15 s (seriam ≈ 5 min com a visibilidade de 60 s da fila).

Eventos de saída vão para `wallet-events.fifo` (`MessageGroupId = walletId`, `MessageDeduplicationId = eventId`), com `eventId`, `eventType`, `version`, `aggregateId`, `correlationId`, `causationId` e `data` tipado. A entrega é **at-least-once**: consumidores devem deduplicar por `eventId` e usar `walletVersion` para ordenação — o contrato não promete ordem financeira entre eventos.

```powershell
./scripts/read-events.ps1 -Count 10    # lê (sem apagar) eventos de wallet-events.fifo com a identidade do consumidor downstream
```

## Testes

Resultados observados nesta máquina sobre o código final (detalhes, defeitos achados e a auditoria de finalização em [docs/IMPLEMENTATION_PROGRESS.md](docs/IMPLEMENTATION_PROGRESS.md) e [docs/FINAL_VALIDATION_REPORT.md](docs/FINAL_VALIDATION_REPORT.md)):

| Execução | Resultado |
| --- | --- |
| `go vet` (padrão, `integration`, `e2e`), `gofmt`, `go test ./...` no host | Limpos; 73 testes unitários passam |
| `go test -race -tags integration ./...` no container, PostgreSQL real | 115 passam (73 unitários + 42 de integração); nenhuma "DATA RACE" |
| E2E (21 testes, 20 ativos) na pilha padrão: MiniStack, três instâncias, Keycloak e PostgreSQL reais, `E2E_REQUIRE_BROKER_IAM=1`, 431 s | **20 aprovados, 1 pulado por desenho** (`TestNoDataRacesWereReported`, que exige instâncias `-race`) |
| E2E com as três instâncias **compiladas com `-race`** e `E2E_RACE=1` | **21/21 aprovados** (inclui `TestNoDataRacesWereReported`), 325 s: as três instâncias compiladas com `-race` atravessaram a suíte inteira (carga, caos, `FAILED`, token expirado) sem reportar "DATA RACE" |
| **Checkout limpo** (`git clone`, projeto Compose e volumes novos): `docker compose up --build` literal, exemplos HTTP e SQS deste README e E2E | Sobe do zero em 262 s (PostgreSQL pelo bootstrap, migrations, filas e identidades, realm do Keycloak importado); exemplos HTTP e SQS deste README com os resultados descritos; **E2E 20 aprovados + 1 pulado por desenho** (374 s); parada graciosa com saída 0 |
| Carga mista de 20 s e **caos** de 40 s (instâncias mortas com SIGKILL) na pilha padrão | 967 e 2 122 operações (HTTP + SQS) aplicadas uma única vez, saldos recalculados = armazenados; latência HTTP p50 148–178 ms, p95 ≈ 0,85–0,90 s, p99 1,6–1,9 s (observação, não benchmark) |

```powershell
# Unitários e análise estática (host)
go vet ./...
go test ./...

# Unitários + integração com -race, em container (tem compilador C), contra o PostgreSQL real
docker compose -f compose.yaml -f compose.test.yaml run --rm tests go test -race -tags integration -count=1 ./...

# Fim a fim: três instâncias, Keycloak, PostgreSQL e broker reais, com injeção de falhas via Docker
docker compose --profile e2e up -d --build --wait app app-2 app-3
$env:E2E_REQUIRE_BROKER_IAM = '1'   # falha (em vez de pular) se o broker não aplicar as políticas
go test -tags e2e -count=1 -v -timeout 40m ./tests/e2e/

# Fim a fim com as instâncias compiladas com -race (a suíte falha se alguma reportar "DATA RACE")
docker compose -f compose.yaml -f compose.race.yaml --profile e2e up -d --build --wait app app-2 app-3
$env:E2E_RACE = '1'; go test -tags e2e -count=1 -v -timeout 40m ./tests/e2e/

# Mesma suíte contra o LocalStack Community (não aplica IAM: o teste da matriz de permissões pula)
docker compose -f compose.yaml -f compose.localstack.yaml --profile e2e up -d --build --wait app app-2 app-3
```

### O que cada camada usa de real e de falso

| Camada | Infraestrutura | Prova | Não prova |
| --- | --- | --- | --- |
| Unitária (`go test ./...`) | Nenhuma (alguns pacotes usam verificador ou fila falsos) | Regras de domínio, validação, mapeamento HTTP, autorização, parsing, métricas | Comportamento com banco, broker ou IdP reais |
| Integração (`-tags integration`; `internal/store`, `internal/messaging`, `cmd/server`) | **PostgreSQL real** com a role de runtime `jungle_app` (grants e *triggers* valem). `internal/messaging` usa uma **fila SQS falsa** e um verificador falso; `cmd/server` usa substitutos do IdP e do broker e **todos os módulos reais** | Atomicidade, constraints, inbox/outbox, leases, recuperação, `FAILED`, início e parada da composição Fx com liberação de recursos | O protocolo SQS real (coberto pelo E2E) |
| E2E (`-tags e2e`; `tests/e2e`) | **PostgreSQL, Keycloak, broker SQS e três instâncias reais**, com falhas injetadas pelo Docker (`stop -t 0` = SIGKILL, `pause`, `kill`) e uma identidade IAM por papel | Tudo entre processos: concorrência, recuperação, DLQ, políticas do broker, tokens reais (inclusive expirados) | Escala: não é benchmark |

- **Integração**: usa a role de runtime, então também exercita grants e *triggers*. Banco `jungle_test`, migrado pelo serviço `migrate-test`; variáveis `TEST_DATABASE_URL` / `TEST_ADMIN_DATABASE_URL` (padrão `127.0.0.1:15432`). Os testes que levam uma carteira ao estado "versão esgotada" ou injetam uma falha do PostgreSQL usam a conexão do dono, que a aplicação nunca usa. O teste da composição Fx cria (e remove) um banco próprio.
- **E2E**: o teste roda no host e controla os containers com a CLI do Docker. Pré-requisito: a pilha de três instâncias acima. `E2E_ADMIN_DATABASE_URL` (padrão `127.0.0.1:15432/jungle`, dono) só serve à injeção de falha do teste de `FAILED`.
- **Carga mista e caos** (`tests/e2e/load_test.go`): 12 workers HTTP + 2 produtores SQS misturam `BET`/`WIN`/`LOSS`/`REFUND`/`ROLLBACK` em 10 carteiras com saldo baixo (para exercitar a proteção contra saldo negativo), com cópias duplicadas concorrentes e reconciliação durante a carga. Ao final, o saldo de cada carteira é **recalculado só a partir das transações persistidas** e comparado com o saldo armazenado, a API, o ledger (versões contíguas, sem saldo negativo) e os eventos entregues. No teste de caos, instâncias são mortas com SIGKILL a cada poucos segundos e os clientes repetem a mesma chave. `E2E_LOAD_SECONDS` ajusta a duração (20 s / 40 s por padrão) e `E2E_LOAD_SEED` repete as escolhas aleatórias. O teste imprime vazão, p50/p95/p99 da latência HTTP, desfecho por status, rejeições por código, conflitos de concorrência, pico e esvaziamento do backlog da outbox. São **observações desta máquina (VM do Docker), não benchmark**.

O que a suíte comprova está mapeado, requisito a requisito, na matriz de [docs/CHALLENGE_ANALYSIS.md](docs/CHALLENGE_ANALYSIS.md).

## Observabilidade

Logs JSON com `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId` quando disponíveis (sem tokens nem payloads financeiros completos). `/metrics` (Prometheus, exige o cliente interno) expõe, entre outras: `wager_transactions_total{status,kind}`, `idempotent_replays_total`, `idempotency_conflicts_total`, `processing_retries_total{reason}`, `wager_processing_duration_seconds{transport="http|sqs"}`, `wallet_lock_wait_seconds` (histograma da espera pelo lock da carteira), `concurrency_conflicts_total` (transações repetidas após deadlock/falha de serialização; deve ficar em 0), `wallet_reconciliation_mismatches_total`, `outbox_pending_events`, `outbox_oldest_pending_age_seconds`, `sqs_dlq_messages`. `/health/live` não depende de nada externo; `/health/ready` verifica PostgreSQL e as filas de entrada e saída e devolve `503` durante o encerramento.

## Controle de acesso do broker (IAM)

O desafio pede que o acesso às filas seja controlado "por credenciais e políticas do broker". O provisionamento cria **uma identidade IAM por papel**, com a política mínima, e o broker padrão (MiniStack com `AUTH=true`) **a aplica**:

| Identidade | Pode | Recusado (coberto por teste) |
| --- | --- | --- |
| `provider-a`, `provider-b` | `SendMessage` em `wager-transactions.fifo` | receber/apagar/purgar a entrada; enviar ou receber na saída; ler a DLQ |
| `jungle-worker` (a aplicação) | receber/apagar/ajustar visibilidade na entrada; `SendMessage` na saída; `GetQueueAttributes` (entrada, saída, DLQ de entrada) | **enviar para a entrada** (forjar pedidos), receber a própria saída, ler ou enviar na DLQ, purgar |
| `events-consumer` | receber/apagar na saída | enviar eventos, tocar na entrada, purgar |
| `dlq-operator` | receber/apagar nas DLQs | enviar tráfego, tocar nas filas de trabalho |
| chave desconhecida | — | tudo |

Evidência: o provisionamento sonda o broker com 8 chamadas somente de leitura e grava o resultado em `.local/broker-security.json` (`iamEnforcementObserved`); o teste E2E `TestBrokerEnforcesQueuePermissionsPerIdentity` exercita 24 recusas (incluindo `PurgeQueue` e `SendMessage` fora do papel) e as permissões de que cada papel precisa. Ele só executa chamadas que alteram filas depois de provar, por leituras inofensivas, que o broker de fato recusa — num broker que não aplica política, **pula** (ou **falha**, com `E2E_REQUIRE_BROKER_IAM=1`) em vez de declarar uma fronteira que não existe. A suíte inteira roda com as identidades por papel, então as permissões concedidas também são provadas **suficientes**.

Limites conhecidos, sem rodeios:

- O emulador **não verifica o segredo** da chave de acesso (uma chave existente é aceita com qualquer segredo); recusa chaves inexistentes e aplica a política da identidade.
- `SenderId` é o id da conta para todos os remetentes. O que identifica o **provedor** é o JWT validado pelo consumidor, cujo `client id` entra no hash da inbox.
- O MiniStack autoriza pelo **nome da chamada**: `DeleteMessageBatch` exige `sqs:DeleteMessageBatch` (na AWS real o lote é coberto por `sqs:DeleteMessage`). A política concede as duas formas. A primeira execução da suíte contra este broker falhou exatamente aí — o consumidor de eventos da suíte apagava em lote — o que também mostra que a política é de fato avaliada.
- `docker compose -f compose.yaml -f compose.localstack.yaml ...` usa o LocalStack Community, que **não aplica IAM** (nem com `ENFORCE_IAM=1`, verificado na 4.14.0): lá o controle de acesso das filas é declarado, não provado.
- Em AWS real, somam-se o mapeamento `SenderId` → provedor e políticas por conta/papel — **não implementado nem provado aqui**.

## Limitações e decisões que você precisa conhecer

- **O isolamento entre provedores não vem do broker**: todos os remetentes aparecem com o mesmo `SenderId` (a conta). A autorização é feita pela aplicação: o JWT do provedor viaja no atributo `Authorization` da mensagem, é validado pelo consumidor (assinatura, *issuer*, *audience*, validade) e o `client id` autenticado entra no hash da inbox como identidade estável do emissor. O broker restringe **o que cada papel pode fazer nas filas** (seção acima), não quem é o provedor.
- Keycloak em modo desenvolvimento, HTTP e segredos de exemplo: somente ambiente local.
- Uma carteira por jogador/moeda; endpoints em BRL; reversões integrais. **Sem partidas dobradas e sem tracing distribuído (OpenTelemetry)**: são bônus; o ledger de lançamentos simples já é auditável e reconciliável, e a correlação ponta a ponta usa `correlationId` (HTTP → inbox → eventos). Não foram feitos de propósito: exigiriam migração de esquema e novas dependências em um sistema cujo caminho financeiro já está provado. Não há *benchmark* de vazão: a carga mista acima é um teste de **consistência** e seus números são observações desta máquina, não metas nem garantias.
- Eventos são at-least-once e podem chegar fora de ordem entre publishers concorrentes.
- **Espaço em disco:** Docker Desktop, imagens, volumes e builds exigem folga em `C:` (recomendado ≥ 5 GB livres). Com o disco cheio o daemon cai com `input/output error` — foi o que ocorreu durante o desenvolvimento.
- **Relógio:** a VM do Docker Desktop pode dar saltos de relógio. A aplicação não depende da ordem entre `created_at` e `updated_at` (ver [ARCHITECTURE.md §16](ARCHITECTURE.md)), e todos os timestamps de uma carteira vêm do relógio do banco.
- **Reinício de instância:** o encerramento gracioso espera o *long-poll* do SQS em andamento terminar (até ~20 s) para não abandonar uma mensagem entregue a uma requisição cancelada; instâncias mortas à força (SIGKILL) podem reter mensagens em voo até o fim da visibilidade (60 s) — nada se perde, a mensagem só atrasa.

## Estrutura

```text
cmd/server/            composição Fx e ciclo de vida
internal/domain/       Money, comandos, regras (Evaluate), carteira, eventos — sem I/O
internal/store/        PostgreSQL: transações, inbox, outbox, ledger, reconciliação, workers
internal/application/  casos de uso e autorização (compartilhados por HTTP e SQS)
internal/httpapi/      rotas, validação estrita, erros padronizados
internal/messaging/    consumidor SQS, envelope, publisher da outbox
internal/workers/      referências pendentes e gauges de observabilidade
internal/auth/         validação OIDC/JWT
internal/trace/        propagação segura de correlation ID
migrations/            esquema, constraints, triggers e grants
deploy/                realm Keycloak, provisionamento do broker (imagem, filas, IAM), bootstrap Postgres
scripts/               publicar mensagem SQS e ler eventos (PowerShell)
tests/e2e/             suíte fim a fim com injeção de falhas
```
