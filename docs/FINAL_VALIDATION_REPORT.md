# Relatório final de validação

Auditoria de finalização do desafio **Jungle Gaming Backend (Go)**, feita em 10/10/2026 sobre o código existente: nada foi reescrito; as alegações do registro anterior (nota estimada 94,5/100; 99 testes = 44 unitários + 37 de integração + 18 E2E) foram **verificadas por execução** e as lacunas encontradas foram corrigidas ou documentadas. Todo número abaixo vem de uma execução registrada nesta rodada; o que não foi provado aparece como tal.

Este relatório é o ponto de partida para quem vai avaliar, publicar ou dar continuidade ao projeto. O [README](../README.md) explica como usar; o [ARCHITECTURE](../ARCHITECTURE.md) registra as decisões (§16 traz as divergências verificadas); a [matriz de evidências](CHALLENGE_ANALYSIS.md) liga cada requisito ao teste que o exercita; o [registro de execução](IMPLEMENTATION_PROGRESS.md) guarda os resultados e os defeitos achados.

## 1. Veredito (três avaliações separadas)

| Dimensão | Avaliação | Base |
| --- | --- | --- |
| **Completude funcional** | **Completa** para os requisitos obrigatórios e para os 10 critérios eliminatórios do enunciado. Nenhum requisito obrigatório ficou sem implementação; as lacunas encontradas na auditoria (abaixo) foram fechadas | §2 e §3 |
| **Nota estimada pela rubrica oficial** | **≈ 95 / 100** (faixa plausível 92–97). É uma **estimativa minha**, não uma avaliação externa. Antes da auditoria eu a estimava em 94,5, com dois pontos que um avaliador atento descontaria (agregados de domínio não usados e sem `fx.Module`; IAM do broker apenas declarado) | §4 |
| **Prontidão para publicação** | **Pronta, aguardando a sua decisão sobre o destino.** Repositório Git local e independente criado em `jungle_gaming/`, com histórico organizado, sem segredos e com `.gitignore`/`.gitattributes` adequados; **nada foi enviado a nenhum remoto**. Falta só a URL do repositório de destino | §8 |

Os pontos que continuam abertos e que importam (detalhe na §9): **IAM de uma AWS real** e o mapeamento `SenderId` → provedor não foram provados (só o emulador que aplica política, e ele não verifica o segredo da chave); **tracing e partidas dobradas** (bônus) não foram feitos de propósito; **sem benchmark** de vazão (a carga é teste de consistência).

## 2. O que a auditoria encontrou (e o que foi feito)

Classificação: **O** = eliminatório, **M** = obrigatório, **P** = pontuado, **B** = bônus, **I** = melhoria sem benefício direto para a avaliação.

| # | Achado | Classe | Situação inicial | O que foi feito | Prova |
| --- | --- | --- | --- | --- | --- |
| 1 | Controle de acesso do broker só **declarado**: o LocalStack Community não aplica IAM (nem com `ENFORCE_IAM=1`; medido na 4.14.0) | O/M | Policies existiam, nada as impunha | Perfil com **MiniStack 1.5.25** (`AUTH=true`), que avalia as políticas das IAM users no SQS: 5 identidades por papel, política mínima, sondagem no provisionamento, E2E da matriz de permissões. Tornado o broker **padrão** (o enunciado admite LocalStack ou MiniStack) | `TestBrokerEnforcesQueuePermissionsPerIdentity` (24 recusas + 6 permissões); `.local/broker-security.json`; a suíte inteira passa com as identidades por papel (permissões suficientes) |
| 2 | A primeira execução da suíte E2E contra o broker que aplica política **falhou em 3 testes** | M | — | Causa: o consumidor de eventos da suíte apagava mensagens em lote e o MiniStack autoriza pelo **nome da chamada** (`sqs:DeleteMessageBatch`); o *sink* da suíte ignorava o erro. Política ajustada e o *sink* passou a relatar cada erro distinto | Os 3 testes passam; o fato também prova que a política é avaliada |
| 3 | `FAILED` sem produtor | M | Estado modelado, nunca produzido; versão de carteira esgotada devolvia `PROCESSING_FAILED` (HTTP `400`) sem registro | **Um** produtor legítimo e estreito (`WALLET_VERSION_EXHAUSTED`); erros de banco, timeouts e falhas do publisher **nunca** viram `FAILED` (análise em ARCHITECTURE §16.5) | unitários, integração real (inclusive worker de referências) e `TestPermanentFailureIsRecordedReturnedAndDeadLettered` |
| 4 | Agregados de domínio existiam mas **não eram usados** pela persistência | P (modelagem) | `WagerTransaction`, `WalletLedgerEntry`, abertura: só em testes | O store valida cada transição no agregado, constrói o lançamento e a abertura pelo domínio; o relógio de transição é limitado, não recusado | testes de domínio e toda a suíte de integração |
| 5 | Sem `fx.Module`; sem teste de **início/parada da composição** com liberação de recursos | M | `register` monolítico; só `fx.ValidateApp` | Um módulo por responsabilidade; workers num *value group* supervisionado; HTTP registrado por último (para a parada drenar primeiro); `Listen` na partida | `TestApplicationStartsServesAndReleasesEverythingOnStop`, `TestAFailedStartReleasesWhatWasAlreadyAcquired` (com mutação verificada) |
| 6 | Testes unitários exigidos ausentes: transições, equação do ledger, abertura e eventos | M | cobertura de domínio só nas regras | 29 testes unitários novos; cobertura do domínio 89,6 % | `internal/domain/*_test.go` |
| 7 | Credenciais **expiradas** não testadas com IdP real | M | só unitário com chaves geradas | E2E com o Keycloak real: tempo de vida encurtado pela API administrativa (e restaurado), HTTP `401` nas 3 instâncias, SQS → DLQ sem efeito | `TestExpiredTokensFromTheRealIdentityProviderAreRefused` |
| 8 | Worker de referências repetia um erro persistente a cada 30 s, sem fim e na mesma cadência | P | lease expirava e voltava | *Backoff* exponencial com jitter (teto 5 min) também para erros; o erro nunca decide o destino da transação | teste com falha **real** do PostgreSQL injetada por *trigger* numa única linha |
| 9 | Mensagem "veneno" retinha o grupo FIFO ≈ 5 min | P | 5 × 60 s de visibilidade | Visibilidade de 2 s para falhas permanentes: DLQ em ≈ 10–15 s | `TestUntrustedSQSMessages…` caiu de 76 s para 14 s |
| 10 | Latência de processamento só medida no HTTP | P (observabilidade) | métrica sem o caminho SQS | rótulo `transport` (`http`/`sqs`) | `TestProcessingLatencyIsSplitByTransportFromTheStart` |
| 11 | Vazamento de `pgxpool` quando o primeiro `Ping` falha | M (liberar recursos) | pool não fechado | `Close` antes de devolver o erro | corrigido durante a refatoração; **sem teste dedicado** |
| 12 | Teste de integração **instável sob carga** (`TestLockWaitIsReported…`: dormia um tempo fixo contado do início da goroutine) | I | falhou 1× sob `-race` com a máquina ocupada (414 ms < 500 ms) | espera determinística: o teste só segura o lock pelo tempo conhecido depois que `pg_blocking_pids` mostra a operação bloqueada | 3 execuções seguidas + suíte `-race` |
| 13 | Retry idempotente da disputa 80 × 80 e reversão pendente após reinício total sem asserção explícita | M | contagens e saldo apenas | asserções do ledger (2 lançamentos), reenvio das **duas** chaves em outra instância, pendência que conclui depois do SIGKILL das 3 instâncias | `TestTwoEightyBetsOnAHundredWallet`, `TestRecoveryAfterEveryInstanceRestarts` |

Itens **avaliados e deliberadamente não feitos**: tracing OpenTelemetry e partidas dobradas (ver §7).

## 3. Os 10 critérios eliminatórios

| Critério eliminatório | Evidência |
| --- | --- |
| Autenticação efetiva nos endpoints de negócio | `TestAuthorizationMatrix` (Keycloak real: sem token, token forjado, expirado — `TestExpiredTokens…`); `internal/httpapi` |
| Acesso não autorizado a operações/transações | Matriz provedor × provedor × cliente interno; replays e consultas escopados ao provedor; `internal/application` |
| Ponto flutuante em dinheiro | `Money` em `int64`, JSON decimal em string; `TestParseMoneyAcceptsOnlyCanonicalDecimals`, `TestMoneyRejectsNonNumericSpecialValues`; nenhum `float` no caminho monetário (revisão) |
| Saldo negativo por concorrência | `TestTwoEightyBetsOnAHundredWallet` (5 rodadas), `TestConcurrentBetsNeverOverdraw` (40 conexões), carga e caos; `CHECK (balance_minor >= 0)` no banco |
| Movimentação duplicada | 50 entregas HTTP+SQS de uma aposta entre 3 instâncias → 1 débito; retries idempotentes; caos com SIGKILL |
| Idempotência só em memória | `TestRecoveryAfterEveryInstanceRestarts` (replay após o reinício das 3 instâncias); chave e inbox no PostgreSQL |
| Dependência de uma única instância | Todos os cenários E2E rodam com 3 instâncias independentes; dois publishers disputando a outbox; worker de outra instância conclui pendência |
| Publicação anterior ao commit | Transactional outbox: evento gravado na mesma transação; `TestEventsSurviveABrokerOutage` (22 eventos retidos, todos entregues depois); `TestCrashBetweenSendAndMarkRepublishesTheSameEventIDs` |
| Ausência de ledger auditável | Ledger append-only (a role de runtime não tem `UPDATE/DELETE`), equação e unicidade no banco; reconciliação que reporta divergência sem repará-la |
| Substituição integral de PostgreSQL, SQS e IdP por mocks | A suíte E2E usa PostgreSQL, Keycloak e broker SQS **reais** em containers; a integração (`-tags integration`) usa PostgreSQL real com a role de runtime |

## 4. Nota estimada, por critério (rubrica oficial)

| Critério | Peso | Estimativa | Justificativa e ressalva |
| --- | ---: | ---: | --- |
| Integridade financeira | 20 | 19–20 | Dinheiro exato, invariantes no banco (triggers/constraints/grants), reversões e reconciliação provadas; `FAILED` agora existe de ponta a ponta |
| Concorrência | 20 | 19–20 | `FOR UPDATE` por carteira, ordem uniforme de locks, 3 instâncias, deadlock repetido e contado, métricas de contenção; sem lock global |
| Idempotência | 15 | 15 | Persistente, conflito detectado, resultado original reproduzido inclusive após reinício total |
| Mensageria e recuperação | 15 | 14–15 | Inbox/outbox/leases/retry/DLQ/parada graciosa provados por interrupções reais; mensagem veneno bloqueia o grupo por ≈ 10–15 s (regra do FIFO) |
| Modelagem e arquitetura | 10 | 9–10 | Domínio encapsulado e agora **usado**; `fx.Module` por responsabilidade; autorização por papel documentada |
| Testes | 10 | 9–10 | 136 testes (73 + 42 + 21), integração real, isolamento, paralelismo, interrupção. Ressalva: a integração do consumidor usa fila falsa (o broker real é coberto pelo E2E) |
| Observabilidade | 5 | 5 | Todas as métricas exigidas, logs JSON com correlação, health checks |
| Documentação | 5 | 5 | README, ARCHITECTURE (decisões e divergências), matriz de evidências, este relatório; comandos executados e conferidos |
| **Total** | **100** | **≈ 95** | Faixa 92–97. Descontos plausíveis: dependência de um emulador menos conhecido; ausência de bônus (partidas dobradas, tracing) que não são pontuados à parte |

## 5. Testes realmente executados nesta rodada

Contagem por `go test -v` (testes de nível superior): **unitários 73** (domain 34, httpapi 7, auth 6, messaging 6, application 4, workers 4, config 3, observability 3, cmd/server 3, trace 2, jsonutil 1), **integração 42** (store 31, messaging 9, cmd/server 2), **E2E 21** (20 ativos + `TestNoDataRacesWereReported`, que só roda contra instâncias `-race`).

| Execução | Resultado |
| --- | --- |
| `gofmt -l`, `go vet ./...` (e com `-tags integration` e `-tags e2e`) | Limpos |
| `go test ./...` no host | 73 passam |
| `go test -race -tags integration -count=1 ./...` no container (PostgreSQL 18.6 real, role `jungle_app`) | **115 passam, 0 falham, 0 puladas**, nenhuma "DATA RACE" |
| E2E completo contra a pilha padrão (MiniStack, 3 instâncias, Keycloak e PostgreSQL reais), `E2E_REQUIRE_BROKER_IAM=1` | **20 aprovados, 1 pulado por desenho**, 431 s (inclui a matriz de IAM, `FAILED`, token expirado, carga e caos) |
| E2E com as três instâncias compiladas com `-race` e `E2E_RACE=1` | **21/21 aprovados**, 325 s, nenhuma "DATA RACE" em nenhum dos três servidores (carga, caos, `FAILED` e token expirado incluídos) |
| Checkout limpo (`git clone`, volumes novos, projeto Compose novo): `docker compose up --build` literal | Sobe do zero em **262 s** (build, PostgreSQL pelo bootstrap, migrations, 4 filas e 5 identidades com `iamEnforcementObserved: true`, realm do Keycloak importado em 109 s). Exemplos HTTP e SQS do README conferidos (`PROCESSED`, `80.00`, v2, replay, `409`, `403`, `401`, reconciliação consistente, SQS → `70.00`). **E2E 20 aprovados + 1 pulado**, 374 s. Parada graciosa: `app` sai com 0 e o comando literal termina com 0 |
| E2E contra o LocalStack Community (`compose.localstack.yaml`) | **19 aprovados, 2 pulados**, 397 s: o provisionamento registra `iamEnforcementObserved: false` e `TestBrokerEnforcesQueuePermissionsPerIdentity` pula de propósito (não há fronteira para provar); o outro pulo é `TestNoDataRacesWereReported`. Tudo o mais passa, inclusive carga e caos |

### Classificação dos testes (o que usam de real e de falso)

| Camada | Infraestrutura | O que prova | O que **não** prova |
| --- | --- | --- | --- |
| Unitária (`go test ./...`) | Nenhuma (alguns usam verificador/fila falsos no pacote) | Regras de domínio, validação, mapeamento HTTP, políticas de autorização, parsing | Comportamento com banco/broker/IdP reais |
| Integração (`-tags integration`) | **PostgreSQL real** com a role de runtime (grants e *triggers* valem). `internal/messaging` usa **fila SQS falsa** e verificador falso; `cmd/server` usa substitutos do IdP e do broker | Atomicidade, constraints, inbox/outbox, leases, recuperação, `FAILED`, ciclo de vida Fx | O protocolo SQS real (coberto pelo E2E) |
| E2E (`-tags e2e`) | **PostgreSQL, Keycloak, broker SQS e 3 instâncias reais**, com falhas injetadas pelo Docker (`stop -t 0`, `pause`, `kill`) | Tudo entre processos: concorrência, recuperação, DLQ, IAM do broker, tokens reais | Escala (não é benchmark) |

Nenhum teste real foi trocado por *mock*; os falsos que já existiam foram mantidos e **classificados**, com o E2E cobrindo o que eles não cobrem.

### Evidência de recuperação

SIGKILL antes do commit; entre commit e ACK (broker congelado); de todas as instâncias; de uma instância; broker congelado e descongelado (eventos retidos na outbox); banco pausado (503 retentável); SIGTERM sob carga (saída 0, cada aposta aplicada uma vez); SIGKILL repetido durante carga mista (caos). Em todos: cada operação aplicada exatamente uma vez, saldo recalculado só das transações persistidas igual ao armazenado, ao da API e ao do ledger, eventos entregues, backlog da outbox drenado.

## 6. Limitações de infraestrutura (sem rodeios)

- **O broker é um emulador.** O MiniStack aplica as políticas de identidade, mas **não verifica o segredo** da chave de acesso e autoriza pelo nome da chamada da API (`DeleteMessageBatch` exige `sqs:DeleteMessageBatch`). Em AWS real o comportamento do IAM é mais rico e o `SenderId` poderia identificar o remetente; isso **não foi provado**.
- **`SenderId` é o id da conta** nos dois emuladores; o provedor é identificado pelo JWT validado pelo consumidor.
- **LocalStack Community não aplica IAM.** O overlay `compose.localstack.yaml` o mantém como alternativa: lá o teste da matriz **pula** (ou falha com `E2E_REQUIRE_BROKER_IAM=1`), porque não há fronteira para provar.
- **Keycloak em `start-dev`, HTTP e segredos de exemplo**: somente ambiente local.
- **Docker Desktop**: precisa de folga em `C:` (≥ 5 GB); com o disco cheio o daemon cai. O primeiro boot do Keycloak depende da memória da VM: ~15 min numa VM de 3,5 GB na rodada anterior, 109 s no checkout limpo desta rodada; ≥ 4 GB é recomendado.
- Os números de carga são **observações desta máquina**, não benchmark.

## 7. Melhorias opcionais **não** feitas, e por quê

| Item | Classe | Decisão |
| --- | --- | --- |
| Tracing OpenTelemetry | B | Não feito. Exigiria novas dependências, um coletor e instrumentação em quatro fronteiras (HTTP, SQS, banco, workers), em um sistema cujo caminho financeiro já está provado. A correlação ponta a ponta existe por `correlationId` (HTTP/`messageId` → inbox → eventos) |
| Partidas dobradas | B | Não feito. Exigiria migração do esquema, reescrita da reconciliação e das constraints, com risco real para o que está provado, e não é pontuado à parte. O ledger de lançamentos simples é append-only, reconciliável e protegido no banco |
| Promover erros transitórios a `FAILED` | — | **Recusado** por regra: um timeout não prova que o commit não ocorreu; "não restaura" já foi um bug nosso (relógio). Só o fato determinístico da versão esgotada produz `FAILED` |
| Cenários financeiros artificiais só para exercitar o enum | — | Recusado: a versão esgotada é o único caso legítimo achado; está documentado por que a ausência dos demais é mais segura |
| Benchmark de vazão (p50/p95/p99 controlados) | B | A carga imprime p50/p95/p99, erros, conflitos e atraso da outbox como **observação**; não há ambiente controlado |

## 8. Git e publicação

- `git rev-parse --show-toplevel` no início apontava para o **repositório pai** (`Área de Trabalho`, remoto `Antonyiuri/trabalhando-com-branches`, ramo `feat/shell-central-aplicacoes-ui`), onde `jungle_gaming/` era uma pasta **não rastreada**. Adicioná-la ali misturaria o projeto com trabalho não relacionado; por isso foi criado um repositório **independente** dentro de `jungle_gaming/` (`git init -b main`), que o repositório pai continua vendo como pasta não rastreada, sem alteração.
- Conteúdo versionado: 84 arquivos de código, migrations, Compose, documentação e testes. **Não** são versionados `.env`, `.local/` (chaves IAM geradas), `.cache/`, `.tools/` (toolchain Go local) nem binários.
- Varredura de segredos nos arquivos versionados: sem chaves privadas, chaves AWS, tokens ou JWTs. Os únicos achados são os **valores descartáveis do ambiente local**, rotulados como tais (`localpassword`, `local-admin-password`, `local-<cliente>-secret`), necessários para o provisionamento reproduzível do IdP e do banco.
- Histórico em inglês, em commits por camada, cada um compilável. **Nenhum remoto foi configurado e nada foi publicado.**
- Para publicar (depois que você escolher o destino): `git remote add origin <URL>` e `git push -u origin main`. Se o destino for público, revise antes a licença e os nomes de pessoas no repositório.

## 9. Problemas remanescentes

1. Prova de IAM em **AWS real** e mapeamento `SenderId` → provedor: não feitos.
2. Os bônus (partidas dobradas, OpenTelemetry) não foram implementados.
3. A integração do consumidor/publisher usa fila falsa; o protocolo SQS real é coberto só pelo E2E, que depende da pilha de containers.
4. Nenhum benchmark de vazão controlado.
5. Mensagem "veneno" ainda bloqueia o grupo FIFO da carteira por ≈ 10–15 s (comportamento do SQS FIFO).

## 10. Como reproduzir

```powershell
Copy-Item .env.example .env
docker compose up --build                                  # comando literal da entrega (primeiro plano)
# ou, para rodar os testes: três instâncias em segundo plano
docker compose --profile e2e up -d --build --wait app app-2 app-3

go vet ./...; go test ./...                                # host (Go 1.27.2)
docker compose -f compose.yaml -f compose.test.yaml run --rm tests go test -race -tags integration -count=1 ./...
$env:E2E_REQUIRE_BROKER_IAM = '1'
go test -tags e2e -count=1 -v -timeout 40m ./tests/e2e/    # com a pilha de 3 instâncias no ar
```

Parar sem apagar dados: `docker compose stop`. **Nunca** use `docker compose down -v` nem `docker volume prune` se quiser preservar o PostgreSQL.
