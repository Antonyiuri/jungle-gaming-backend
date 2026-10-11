# Validação

Comandos e resultados das verificações. Os comandos completos e as variáveis estão no [README](../README.md#testes); a ligação requisito → teste, em [CHALLENGE_ANALYSIS.md](CHALLENGE_ANALYSIS.md).

## Camadas de teste

| Camada | Quantidade | Infraestrutura |
| --- | ---: | --- |
| Unitária (`go test ./...`) | 73 | Nenhuma; alguns pacotes usam verificador ou fila falsos |
| Integração (`-tags integration`) | 42 | PostgreSQL real com a role de runtime. `internal/messaging` usa fila SQS falsa e verificador falso; `cmd/server` usa substitutos do IdP e do broker e todos os módulos reais |
| E2E (`-tags e2e`) | 21 | PostgreSQL, Keycloak, broker SQS e três instâncias reais da aplicação, com falhas injetadas pelo Docker (`stop -t 0`, `pause`, `kill`); `TestNoDataRacesWereReported` só roda contra instâncias `-race` |

Com `-tags integration`, o total de testes de nível superior é 115 (os 73 unitários mais os 42 de integração). Os testes com dublês cobrem lógica isolada; o protocolo SQS real, o Keycloak e a recuperação entre processos são cobertos pelo E2E.

## Versão atual (commit `2074075`)

O nome do módulo Go foi trocado para `github.com/Antonyiuri/jungle-gaming-backend` depois das execuções da seção seguinte. Sobre o commit abaixo, que contém o código final (os commits posteriores só alteram documentação), foram executados:

| Verificação | Resultado |
| --- | --- |
| `gofmt -l`, `go vet ./...` (padrão, `integration` e `e2e`) | Limpos |
| `go test ./...` | 73 passam |
| `go test -tags integration -count=1 ./...` contra o PostgreSQL real, no host | 115 passam em 6 de 7 execuções. Em uma execução, logo depois do E2E e com a máquina carregada, um teste falhou (114/115); não identifiquei qual nem reproduzi a falha nas 6 execuções seguintes |
| E2E completo, pilha padrão (MiniStack, 3 instâncias, Keycloak e PostgreSQL), `E2E_REQUIRE_BROKER_IAM=1`, imagem construída no commit | 20 aprovados, 1 pulado por desenho (`TestNoDataRacesWereReported`), 357 s |

## Execuções anteriores à troca do nome do módulo

Feitas sobre o mesmo código, com o módulo ainda chamado `github.com/junglegaming/backend-challenge-go`. A troca alterou só o `go.mod` e os caminhos de import.

| Verificação | Resultado |
| --- | --- |
| `go test -race -tags integration -count=1 ./...` no container (PostgreSQL 18.6 real, role `jungle_app`) | 115 passam, nenhuma "DATA RACE" |
| E2E com as três instâncias compiladas com `-race` e `E2E_RACE=1` | 21/21 aprovados, 325 s |
| Checkout limpo (`git clone`, projeto Compose e volumes novos): `docker compose up --build` literal | Sobe do zero em 262 s; exemplos HTTP e SQS do README conferidos; E2E 20 aprovados e 1 pulado; parada graciosa com saída 0 |
| E2E contra o LocalStack Community (`compose.localstack.yaml`) | 19 aprovados, 2 pulados: o teste da matriz de permissões pula porque o provisionamento registra `iamEnforcementObserved: false` |
| Carga mista (20 s) e caos (40 s, instâncias mortas com SIGKILL) | 967 e 2 122 operações aplicadas uma única vez; saldos recalculados só das transações persistidas iguais aos armazenados; latência HTTP p50 148–178 ms, p95 ≈ 0,9 s, p99 1,6–1,9 s (observação desta máquina, não benchmark) |

## Reproduzir

```powershell
docker compose up --build                       # comando literal da entrega
go vet ./...; go test ./...                     # host (Go 1.27.2)
docker compose -f compose.yaml -f compose.test.yaml run --rm tests go test -race -tags integration -count=1 ./...
docker compose --profile e2e up -d --build --wait app app-2 app-3
$env:E2E_REQUIRE_BROKER_IAM = '1'; go test -tags e2e -count=1 -v -timeout 40m ./tests/e2e/
```

A CI ([.github/workflows/ci.yml](../.github/workflows/ci.yml)) executa `gofmt`, `go vet` e os testes unitários com `-race`. As limitações do ambiente e do escopo estão em [ARCHITECTURE.md §16](../ARCHITECTURE.md#16-limitações-e-escopo).
