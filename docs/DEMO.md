# Demonstração ao vivo

`scripts/demo.ps1` executa, contra a stack real do Docker (Keycloak, PostgreSQL, MiniStack e três instâncias da aplicação), um roteiro de nove fases. Tudo o que ele imprime vem de respostas HTTP, de `SELECT`s no PostgreSQL e de logs dos containers; um `[PASS]` só aparece depois de uma verificação verdadeira. O script cria carteiras novas (UUIDs e chaves novos a cada execução, prefixo `demo-<id>`) e não altera dados existentes. Não apaga volumes, não faz commits e não envia nada ao GitHub.

## Como executar

1. Abra o Docker Desktop. Reserve ≥ 4 GB de RAM para a VM e ≥ 5 GB de disco livre (ver o [README](../README.md)).
2. No PowerShell, na raiz do repositório, suba a stack com três instâncias:

   ```powershell
   docker compose --profile e2e up -d --build --wait app app-2 app-3
   ```

3. Rode a demonstração:

   ```powershell
   ./scripts/demo.ps1          # direto
   ./scripts/demo.ps1 -Step    # pausa entre as fases (ENTER para continuar)
   ```

   O código de saída é 1 se alguma verificação falhar. Se o Windows bloquear scripts, libere só para a sessão atual, sem mudar a política permanentemente:

   ```powershell
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
   ```

O script assume as portas padrão (`8080`, `8082`, `8083` da API, `8081` do Keycloak, `4566` do broker). Se você as mudou no `.env`, passe `-Api1`, `-Api2`, `-Api3` e `-Keycloak`.

## Fases

| Fase | Ação real | Evidência |
| --- | --- | --- |
| 1 | Serviços, portas e health checks das três instâncias | `/health/live` e `/health/ready` com 200 |
| 2 | Tokens `client_credentials` do Keycloak; negativas de autorização; isolamento entre provedores | 401 sem token, 403 para o provedor em rota interna, provider-a não lê transações do provider-b |
| 3 | Carteira de `100.00`; `BET 20`, `WIN 30`, `LOSS 0`, `REFUND 20`, `ROLLBACK 20` | Saldos `80 → 110 → 110 → 130 → 110`; ledger com 5 lançamentos, o `ROLLBACK` como débito novo |
| 4 | Replay da `BET` e conflito de conteúdo | Mesmo `transactionId`, `idempotentReplay: true`, sem novo lançamento; `409 IDEMPOTENCY_CONFLICT` |
| 5 | Duas apostas de `80.00` sobre `100.00`, em duas instâncias ao mesmo tempo | Uma `PROCESSED`, uma `REJECTED`, saldo `20.00`, um único débito |
| 6 | Mensagem `WagerTransactionRequested` com a identidade IAM e o JWT do provedor | Transação, inbox `ACK`, outbox publicada e eventos na fila de saída |
| 7 | Operação em uma instância, leitura em outra, replay na terceira | Mesmo saldo nas três |
| 8 | Reconciliação, `/metrics` (exige o cliente interno) e logs por `X-Correlation-ID` | Diferença `0.00`; mesmo `correlationId` na resposta, no log e nos eventos |
| 9 | `docker stop -t 0` e `docker start` só da instância `app-2` | As outras seguem respondendo; saldo e idempotência preservados |

## Acompanhar em paralelo

Em outro terminal:

```powershell
docker compose --profile e2e ps
docker compose logs -f --tail 30 app app-2 app-3
docker compose exec -T postgres psql -U jungle -d jungle -c "SELECT status, COUNT(*) FROM wager_transactions GROUP BY status ORDER BY status;"
docker compose exec -T postgres psql -U jungle -d jungle -c "SELECT event_type, COUNT(*) FROM outbox_events GROUP BY event_type ORDER BY event_type;"
```

No navegador: `http://localhost:8080/health/ready`, `:8082` e `:8083`. O `/metrics` não é público; não cole tokens em capturas de tela.

## Particularidades

- O broker é o **MiniStack**, que aplica as políticas IAM por identidade; o provedor da mensagem é estabelecido pelo JWT, validado pelo consumidor (ver [ARCHITECTURE.md §10](../ARCHITECTURE.md)).
- `scripts/publish-sqs.ps1` publica o envelope com a identidade IAM local do provedor. A fase 6 usa o auxiliar `scripts/demo_consume_events.py`, que age como consumidor downstream: lê e apaga **só os eventos da carteira da demonstração**. `scripts/read-events.ps1` apenas lê, sem apagar.
- A integração SQS é assíncrona: o script espera com tentativas limitadas antes de concluir que a operação foi processada.
- A fase 9 não toca nos outros serviços e não há `docker compose down -v` nem `docker system prune`. Se o Docker Desktop travar, reinicie-o manualmente e rode o script de novo; os dados ficam preservados.
- O `[FAIL]` mostra a causa e o script segue, para o resumo final listar o que falhou; ele nunca marca como aprovada uma etapa que falhou.
