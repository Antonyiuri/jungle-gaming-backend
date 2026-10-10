# Progresso da implementação

Atualizado em 10/10/2026 (rodada de finalização). Este registro contém somente resultados observados.

## Situação

**Concluído; auditado e estabilizado em uma segunda rodada.** A primeira rodada (seções "Rodada anterior" abaixo) entregou todos os requisitos funcionais com 99 testes e o LocalStack como broker. A rodada de finalização **não reescreveu nada do que funcionava**: verificou as alegações anteriores por execução, achou lacunas reais (listadas em "Defeitos reais" e no [relatório final](FINAL_VALIDATION_REPORT.md)) e as fechou: IAM do broker agora **aplicado** (MiniStack como broker padrão), `FAILED` com produtor legítimo, agregados de domínio usados pela persistência, composição por `fx.Module` com teste de início/parada, testes unitários exigidos pelo enunciado, token expirado com o IdP real e um repositório Git local organizado.

## Resultados observados (rodada de finalização, código final)

| Verificação | Resultado |
| --- | --- |
| `gofmt -l`, `go vet ./...` (e com `-tags integration` / `-tags e2e`) | Limpos |
| `go test ./...` no host | **73** testes unitários passam (eram 44) |
| `go test -race -tags integration -count=1 -v ./...` no container, PostgreSQL 18.6 real com a role `jungle_app` | **115 passam, 0 falham, 0 puladas** (73 unitários + 42 de integração, eram 37); nenhuma "DATA RACE". A primeira passagem acusou um teste instável (`TestLockWaitIsReported…`, 414 ms < 500 ms com a máquina ocupada); corrigido (espera determinística) e a suíte repetida |
| Primeira execução do E2E contra o broker que **aplica** políticas (MiniStack, `E2E_REQUIRE_BROKER_IAM=1`) | 16 passaram e **3 falharam** (`TestEventsSurviveABrokerOutage`, carga e caos): o consumidor de eventos da suíte apagava em lote e a política não nomeava `sqs:DeleteMessageBatch`; o *sink* ignorava o erro. Corrigido (política + relato de erros) |
| E2E completo, pilha padrão (MiniStack, 3 instâncias, Keycloak e PostgreSQL reais), `E2E_REQUIRE_BROKER_IAM=1`, imagem do código final, 431 s | **20 aprovados, 1 pulado por desenho** (`TestNoDataRacesWereReported`); inclui os 3 testes novos (matriz de IAM, `FAILED`, token expirado) |
| E2E com as três instâncias compiladas com `-race` e `E2E_RACE=1` | **21/21 aprovados** (inclui `TestNoDataRacesWereReported`), 325 s: as três instâncias compiladas com `-race` atravessaram a suíte inteira (carga, caos, `FAILED`, token expirado) sem reportar "DATA RACE" |
| **Checkout limpo** a partir do `git clone` do repositório local, projeto Compose e volumes novos: `docker compose up --build` literal | Sobe do zero em 262 s (build, PostgreSQL pelo bootstrap, migrations, filas e identidades, realm do Keycloak importado em 109 s). Exemplos HTTP e SQS do README: `PROCESSED`, saldo `80.00`, versão `2`; replay com o mesmo `transactionId`; `409` com valor diferente; `403` para provedor lendo carteira e `401` sem token; reconciliação consistente; aposta via SQS processada (`70.00`) e `read-events.ps1` lendo os eventos. **E2E nesse checkout: 20 aprovados + 1 pulado por desenho**, 374 s. Parada graciosa: `app` sai com código 0 (os quatro workers registram `worker stopped`) e o comando literal termina com 0 |
| E2E contra o LocalStack Community (overlay `compose.localstack.yaml`) | **19 aprovados, 2 pulados**, 397 s: o provisionamento registra `iamEnforcementObserved: false` e `TestBrokerEnforcesQueuePermissionsPerIdentity` pula de propósito (não há fronteira para provar); o outro pulo é `TestNoDataRacesWereReported`. Tudo o mais passa, inclusive carga e caos |

Contagens (`go test -v`): unitários 73 (domain 34, httpapi 7, auth 6, messaging 6, application 4, workers 4, config 3, observability 3, cmd/server 3, trace 2, jsonutil 1), integração 42 (store 31, messaging 9, cmd/server 2), E2E 21 (20 ativos + o teste de race). Cobertura de instruções do domínio: 89,6 %.

Observações de carga na pilha padrão (esta máquina, VM do Docker, 3 instâncias; **não são benchmark**): carga mista de 20 s com 967 operações (852 HTTP + 115 SQS), 43 operações HTTP/s, latência HTTP p50 178 ms / p95 842 ms / p99 1,58 s, **0 conflitos de concorrência**, backlog da outbox com pico de 881 eventos e totalmente publicado 19 s depois; caos de 40 s com 2 122 operações e SIGKILL de instâncias, p50 148 ms / p95 902 ms / p99 1,93 s, backlog com pico de 2 325 drenado em 42 s. Em ambas, cada operação foi aplicada uma única vez e o saldo recalculado só das transações persistidas igualou o armazenado.

### Defeitos reais encontrados pela rodada de finalização e corrigidos

| Achado | Como apareceu | Correção |
| --- | --- | --- |
| Controle de acesso do broker só declarado (LocalStack Community não aplica IAM, nem com `ENFORCE_IAM=1`) | Experimento isolado: todas as chamadas permitidas, inclusive com chave desconhecida | Broker padrão passou a ser o MiniStack (`AUTH=true`), 5 identidades por papel, sondagem no provisionamento e matriz de permissões no E2E |
| Política sem `sqs:DeleteMessageBatch`: o MiniStack autoriza pelo nome da chamada | 3 testes E2E falharam por "eventos nunca chegaram"; o laboratório isolado mostrou 10/40 entregues e `AccessDeniedException` no log | Política concede também as variantes em lote; o *sink* da suíte passou a relatar cada erro |
| `FAILED` sem nenhum produtor; versão de carteira esgotada devolvia `PROCESSING_FAILED` (HTTP `400`) sem registrar nada | Auditoria do estado `FAILED` | `Evaluate` produz `FAILED`/`WALLET_VERSION_EXHAUSTED`; persistido, HTTP `500`, replay idêntico, inbox `DLQ`; `PROCESSING_FAILED` passou a `500` |
| Agregados de domínio sem uso pela persistência | Leitura do store | Transições validadas por `WagerTransaction.Apply`, lançamento por `NewWalletLedgerEntry`, abertura por `NewOpeningTransaction` |
| Composição monolítica (`register`) sem `fx.Module` nem teste de início/parada | Requisito do enunciado | Um módulo por responsabilidade; teste que sobe a composição real e prova a liberação de porta, pool, chamadas ao broker e goroutines; teste de partida que falha no meio |
| Pool do banco não fechado quando o `Ping` inicial falha | Refatoração | `Close` antes de devolver o erro |
| Erro persistente no worker de referências repetido a cada 30 s, sem fim | Leitura do worker | *Backoff* também para erros; o erro nunca decide o destino da transação; teste com falha real do PostgreSQL injetada por *trigger* |
| Mensagem de falha permanente retinha o grupo FIFO ≈ 5 min | Leitura do consumidor | Visibilidade de 2 s para mensagens "mortas": DLQ em ≈ 10–15 s |
| Latência de processamento só no HTTP | Revisão contra o enunciado | Rótulo `transport` |
| Testes unitários exigidos ausentes (transições, equação do ledger, abertura, eventos, `NaN`/`Infinity`) | Revisão contra o enunciado | 29 testes unitários novos |
| Token expirado nunca testado com o IdP real | Revisão contra o enunciado | E2E com o Keycloak real (tempo de vida encurtado pela API administrativa e restaurado) |
| `TestLockWaitIsReported…` instável sob carga | Falhou 1× na suíte `-race` | Espera determinística pelo bloqueio (`pg_blocking_pids`) |

## Rodada anterior: resultados observados (LocalStack; antes da auditoria)

| Verificação | Resultado |
| --- | --- |
| `gofmt -l`, `go vet ./...` (e com `-tags integration` / `-tags e2e`) | Limpos |
| `go test ./...` no host | 44 testes unitários passam |
| `go test -race -p 2 -tags integration -count=1 ./...` no container `tests`, PostgreSQL 18.6 real com a role `jungle_app` | Passa em todos os pacotes (44 unitários + 37 de integração) |
| E2E (`go test -tags e2e`, 18 testes) com **as três instâncias compiladas com `-race`** e `E2E_RACE=1`, Keycloak/LocalStack/PostgreSQL reais, ~395 s | **18/18**, inclusive carga, caos e `TestNoDataRacesWereReported`: nenhuma "DATA RACE" nos três servidores |
| **Checkout limpo do código final**: cópia sem `.env`/`.local`/caches/toolchain, projeto Compose e volumes novos, `Copy-Item .env.example .env` + `docker compose up -d --build --wait app app-2 app-3` | Sobe do zero (PostgreSQL pelo bootstrap, realm do Keycloak importado, filas provisionadas) |
| Exemplos HTTP e SQS do README nesse checkout | `PROCESSED`, saldo `80.00`, versão `2`; replay com o mesmo `transactionId`; `409` com valor diferente; reconciliação consistente; aposta via SQS processada (`70.00`); `/metrics` expõe `wallet_lock_wait_seconds`, `concurrency_conflicts_total`, `outbox_pending_events`, `sqs_dlq_messages` |
| E2E nesse checkout limpo (imagem padrão), ~393 s | **17 aprovados, 1 pulado por desenho** (o teste de race, que exige instâncias `-race`) |
| Caos (`TestChaosLoad…`, 40 s, SIGKILL de uma instância a cada 3–6 s) repetido 6 vezes seguidas na stack `-race` (imagem anterior à métrica de conflitos), mais 1 vez na suíte `-race` final e 1 vez no checkout limpo | 8/8; ≈ 18 000 operações e 24 kills nas 6 primeiras; cada operação aplicada uma única vez |
| Migrations `up` → `down 1` → `up` em banco vazio descartável | 5 tabelas → 0 → 5 |
| `docker compose run --rm migrate` / `provision` reexecutados; reinício do LocalStack (as filas somem) + `provision` | "no change" / saída 0; as instâncias voltam a `ready` sem reiniciar |

Contagens (`go test -list`): unitários 44 (domain 13, auth 6, httpapi 6, messaging 6, application 4, config 3, cmd/server 3, trace 2, jsonutil 1), integração 37 (store 28, messaging 9), E2E 18.

O que cada suíte prova, requisito a requisito, está em [CHALLENGE_ANALYSIS.md](CHALLENGE_ANALYSIS.md). Em resumo: dinheiro exato; saldo e ledger atômicos; nenhum saldo negativo sob carga; idempotência compartilhada entre HTTP e SQS e entre três processos; inbox/outbox recuperáveis (crash antes do commit, entre commit e ACK, entre envio e marcação; broker e banco indisponíveis; reinício total; SIGKILL aleatório sob carga); DLQ por *redrive*; autorização por provedor com IdP real; o banco recusa escritas incoerentes mesmo vindas da role de runtime; reconciliação reporta divergência sem repará-la; deadlock repetido de forma transparente.

### Observações de carga (esta máquina, VM do Docker, 3 instâncias; **não são benchmark**)

- Carga mista de 20 s: 1 400–2 300 operações (HTTP + SQS), ≈ 45–115 operações HTTP/s dependendo de a stack estar com `-race` e da carga do host; latência HTTP p50 ≈ 80–200 ms, p95 ≈ 280–670 ms, p99 ≈ 450–1 060 ms.
- O backlog da outbox chegou a 1 700–4 600 eventos durante a carga e foi totalmente publicado e o gauge voltou a 0 em ≈ 16–55 s depois dela: o publisher acompanha, mas não à taxa de ingestão.
- As rejeições da carga são as esperadas (`INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `REFERENCE_ALREADY_REVERSED`, `REFERENCE_KIND_NOT_ALLOWED` e os `REFERENCE_AMOUNT_MISMATCH` provocados de propósito).

## Rodada anterior: defeitos reais encontrados pela validação e corrigidos

Cada correção abaixo tem teste de regressão que **falha sem ela**.

| Achado | Como apareceu | Correção |
| --- | --- | --- |
| **Carteira inutilizável por salto/desvio de relógio.** `RestoreWallet` recusava `updated_at < created_at` (`INVALID_WALLET`) e toda operação começa restaurando a carteira, então ela ficava bloqueada para sempre (HTTP `400`; SQS → DLQ) | E2E no checkout limpo: um *retry* devolveu `400 INVALID_WALLET`. O banco tinha uma carteira com `updated_at` 16,7 s **antes** de `created_at`; a ordem de chegada dos logs do app-3 mostrou o relógio da VM do Docker pulando ~17 s para trás e voltando. `created_at` vinha do relógio da aplicação e `updated_at` do do banco | O domínio normaliza em vez de rejeitar (saldo negativo, versão < 1, IDs inválidos e `created_at` ausente continuam inválidos); `CreateWallet` usa o relógio do banco; mesma regra em `RestoreWagerTransaction`. Em produção, alguns ms de desvio entre app e banco já bastariam |
| Consumidor: `ChangeMessageVisibility`/`DeleteMessage` reutilizavam o prazo de 10 s do processamento, já esgotado num timeout do banco — o atraso de 5/10/20/40 s nunca era aplicado (a mensagem esperava a visibilidade cheia, 60 s) | Revisão do `handle` ao investigar um atraso de 60 s no E2E | Prazos independentes para processar e para registrar o resultado (e para cada sonda do `Observer`); falha ao ajustar a visibilidade agora é registrada |
| Encerramento cancelava o *long-poll* em andamento: o broker ainda entregava uma mensagem à requisição abandonada, invisível por 60 s | E2E: "consumidor bloqueado `1m0.2s` após publicar", logo depois de um `SIGTERM`; o log mostrou as 5 mensagens tratadas exatamente 60 s depois | O consumidor deixa o poll em andamento terminar (≤ ~20 s) e trata o que vier; depois do ajuste o mesmo teste mostrou `100ms` |
| Publisher da outbox não gravava o *retry* quando o `SendMessage` consumia o prazo inteiro | Logs do E2E com o broker congelado: `outbox retry scheduling failed … context deadline exceeded` | Contextos independentes para envio e escrita do resultado |
| Laço de repetição de deadlock (`40P01`/`40001`) **sem nenhum teste**; `wallet_lock_wait_seconds` e `concurrency_conflicts_total` previstas e não implementadas | Revisão de lacunas quando houve folga de disco | Métricas implementadas; teste que constrói o deadlock de verdade e prova a repetição transparente (sem a repetição o chamador recebe `deadlock detected (SQLSTATE 40P01)`) |
| Script de publicação SQS usava `MessageGroupId = provedor` (contrato: `walletId`) | O consumidor rejeitou a primeira mensagem real | `publish.py` e `publish-sqs.ps1` seguem o contrato |
| JSON da carteira em PascalCase | Smoke test | Tags JSON + teste |
| Healthcheck do Postgres usava o socket Unix e "passava" durante o *init* | Primeiro `docker compose up` (`migrate` com `connection refused`) | `pg_isready -h 127.0.0.1` |
| `.dockerignore` sem `.cache`; três serviços com `build` e o mesmo tag em paralelo | Contexto de build enorme; `image … already exists` | `.cache` ignorado; só `app` constrói |
| `fx.NopLogger` escondia falhas de startup; `StopTimeout` do Fx (15 s) < `SHUTDOWN_TIMEOUT` (30 s); grace do Compose = shutdown | Revisão | Logger Fx só com erros; `StopTimeout = SHUTDOWN_TIMEOUT + 5 s`; `stop_grace_period: 45s` |
| Gauges da outbox e da DLQ nunca atualizados; readiness só olhava a fila de entrada | Revisão contra a arquitetura | `workers.Observer` (5 s) + readiness das duas filas |
| `X-Correlation-ID` aceitava qualquer valor; propagá-lo aos eventos faria a operação financeira falhar com caracteres de controle | Revisão | Formato estrito `[A-Za-z0-9._:-]{1,128}` (HTTP e `messageId`) |
| Um *panic* em qualquer worker derrubaria o processo inteiro | Revisão ("goroutines supervisionadas") | `supervise`: recupera, registra a pilha e reinicia o loop |
| .NET do PowerShell não resolve `*.localhost` | Smoke test | Token obtido em `127.0.0.1` (o `iss` é fixado pelo hostname do Keycloak) |

Defeitos *de teste* (não do sistema) também corrigidos: IDs fixos em banco persistente, `t.Setenv` vazando entre casos, limiar "≥ 2 commits" incorreto para FIFO (o grupo fica retido enquanto a mensagem em voo não é apagada), ordem dos `Cleanup`, restauração de carteira corrompida de propósito (o próprio banco recusou o `UPDATE` de volta), `TEST_ADMIN_DATABASE_URL` ausente no container, espera de 60 s curta demais para o atraso de entrega (agora 150 s, com diagnóstico), e — no caos — o cliente de teste desistia após ~600 ms de tentativas, menos que o tempo de um restart com duas instâncias fora do ar (agora insiste, em ordem, por até ~30 s). O sink de eventos do teste passou a usar vários receptores e `DeleteMessageBatch`: um consumidor único era o gargalo das asserções de eventos, não os publishers.

## Incidente do Docker Desktop (resolvido)

O daemon caiu três vezes durante builds (`failed to receive status: rpc error: code = Unavailable … EOF`, sem processo `vmmem`). **Causa-raiz medida: o disco `C:` do host estava com 0,00 GB livres** — o disco virtual do Docker (`docker_data.vhdx`, 41,7 GB na época) não conseguia crescer, o build mostrou `input/output error` e `bus error`, e o daemon caía. A hipótese inicial (memória da VM, 3,5 GB, com o host a ~0,7 GB de RAM livre) estava incompleta. Nada do usuário foi apagado (sem `prune`, `down -v` em dados reais, remoção de imagens/volumes ou `wsl --shutdown`); só o cache de build do Go local do projeto e os artefatos do meu projeto descartável de validação (`jungle-clean`: containers, rede e o volume `jungle-clean_postgres-data`, com o alvo verificado antes, nas três vezes) foram removidos. Depois que o usuário liberou espaço (duas vezes), o Docker Desktop foi reiniciado e tudo foi executado.

Pegada deste trabalho depois da última rodada: `docker_data.vhdx` em 45,3 GB (+3,6 GB), volumes do projeto de 0,9 GB no total (`postgres-data` 459 MB com os dados dos testes, caches Go 440 MB), caches do projeto no host 0,5 GB + toolchain 0,3 GB. O `docker system df` mostra 7,5 GB de cache de build e 25 GB de imagens recuperáveis, em sua maior parte de outros projetos: decisão do dono da máquina.

## Estado ao encerrar

- Stack do projeto **parada** (`docker compose stop`); volume `jungle-gaming_postgres-data` preservado (dados de teste). Para religar: `docker compose up -d --build --wait app` (ou o perfil `e2e` com as três instâncias). A imagem local `jungle-gaming-backend:local` é a do código final.
- Repositório Git **local e independente** em `jungle_gaming/` (`main`), commits por camada em inglês; **nenhum remoto configurado, nada publicado**. O repositório pai (`Área de Trabalho`) continua vendo `jungle_gaming/` como pasta não rastreada, sem alterações.

## Limites assumidos (não são pendências de teste)

IAM de uma **AWS real** e o mapeamento `SenderId` → provedor (só o emulador que aplica política foi exercitado; ele não verifica o segredo da chave); sem *benchmark* de vazão controlado (os números acima são observações); **sem tracing distribuído e sem partidas dobradas**, bônus deixados de fora de propósito (ver [relatório final](FINAL_VALIDATION_REPORT.md) §7); instâncias mortas à força (SIGKILL) podem reter mensagens em voo até o fim da visibilidade (60 s): nada se perde, só atrasa.
