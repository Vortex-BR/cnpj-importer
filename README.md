# Serviço de Importação de CNPJs

Serviço dedicado para baixar os Dados Abertos de CNPJ da Casa dos Dados/Receita
Federal, processar os ZIPs em streaming e manter uma base PostgreSQL de
estabelecimentos ativos.

O serviço importa matrizes e filiais como CNPJs independentes. Não baixa
arquivos de sócios e não persiste CPF, QSA, telefone, e-mail, contatos pessoais
ou endereço completo.

## Arquitetura

- FastAPI expõe endpoints administrativos.
- Um worker interno consulta a fila persistida no PostgreSQL.
- Um scheduler opcional verifica novos snapshots completos, sem fazer backfill.
- PostgreSQL advisory lock impede duas importações simultâneas.
- Downloads retomáveis ficam em `/data/cache/<source_month>`.
- Empresas, MEIs e tabelas de domínio usam staging `UNLOGGED`.
- Estabelecimentos são filtrados e processados em batches sem extração do ZIP.
- Cada batch confirma dados e checkpoint na mesma transação.
- O CNPJ completo é a chave primária e todo merge usa `ON CONFLICT (cnpj)`.

Uma importação interrompida pode ser reiniciada. Empresas e referências são
recarregadas no staging; arquivos de estabelecimentos continuam após a última
linha confirmada.

## Requisitos operacionais

- PostgreSQL 13 ou superior.
- Volume persistente montado em `/data`.
- Recomendação inicial de pelo menos 10 GB livres no volume de cache.
- Espaço adicional no PostgreSQL para `companies`, índices e staging.
- Uma réplica do app é suficiente. Mais réplicas são protegidas pelo advisory
  lock, mas não aceleram uma única importação.

Os arquivos obrigatórios somam vários gigabytes. A primeira carga pode durar
horas, dependendo de rede, CPU, disco e capacidade do PostgreSQL.

## Deploy no EasyPanel

1. Crie um novo serviço do tipo App usando este repositório ou Dockerfile.
2. Exponha a porta `8000`.
3. Monte um volume persistente no caminho `/data`.
4. Configure as variáveis descritas em `.env.example`.
5. Use o hostname interno do PostgreSQL na `DATABASE_URL`, por exemplo:

```text
postgresql://user:password@leads-postgres:5432/leads
```

6. Faça o deploy. O comando padrão da imagem é:

```text
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Na inicialização, o app abre o pool PostgreSQL, aplica a migration idempotente e
inicia o worker e, se habilitado, o scheduler automático. Se a tabela
`companies` já existir, as colunas e índices faltantes são adicionados.

Não coloque a senha do banco ou o token no repositório. Gere um
`IMPORT_API_TOKEN` longo e aleatório no EasyPanel.

## Variáveis de ambiente

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `DATABASE_URL` | obrigatório | URL do PostgreSQL |
| `IMPORT_API_TOKEN` | obrigatório | Token dos endpoints administrativos |
| `CASA_DOS_DADOS_BASE_URL` | URL oficial | Origem dos arquivos |
| `DATA_DIR` | `/data` | Raiz do volume persistente |
| `CATEGORIES_CONFIG` | `config/categories.yml` | Regras de CNAE |
| `BATCH_SIZE` | `5000` | Linhas processadas por checkpoint |
| `DOWNLOAD_TIMEOUT` | `120` | Timeout HTTP em segundos |
| `DOWNLOAD_RETRIES` | `3` | Tentativas por arquivo |
| `MAX_WORKERS` | `2` | Downloads paralelos |
| `DB_POOL_MAX_SIZE` | `4` | Máximo de conexões; mínimo 2 para importação + heartbeat |
| `AUTO_IMPORT_ENABLED` | `false` | Ativa a verificação automática mensal |
| `AUTO_IMPORT_CHECK_INTERVAL_SECONDS` | `21600` | Intervalo independente do scheduler, padrão de 6 horas |
| `AUTO_IMPORT_CHECK_JITTER_SECONDS` | `300` | Jitter aleatório máximo do scheduler |
| `AUTO_IMPORT_MAX_RETRIES_PER_MONTH` | `3` | Limite de tentativas automáticas com falha por snapshot |
| `AUTO_IMPORT_RETRY_BACKOFF_SECONDS` | `86400` | Espera após falha automática antes de nova tentativa |
| `WORKER_POLL_INTERVAL_SECONDS` | `5` | Intervalo exclusivo da checagem da fila |
| `RUN_STALE_TIMEOUT_SECONDS` | `21600` | Tempo sem heartbeat para recuperar runs órfãs |
| `CACHE_RETENTION_DAYS` | `45` | Idade mínima para limpeza |
| `MAX_ERROR_SAMPLES` | `1000` | Máximo de erros detalhados por run |
| `LOG_LEVEL` | `INFO` | Nível de log |

## Endpoints

`GET /health` é público. Todos os demais endpoints exigem:

```text
X-Import-Token: <IMPORT_API_TOKEN>
```

### Saúde

```bash
curl https://cnpj.example.com/health
```

Retorna `503` quando o PostgreSQL não está acessível.

Além do banco e do worker, a resposta informa `auto_import_enabled`,
`last_auto_check_at`, `last_auto_check_result`, `next_auto_check_at` e
`last_detected_source_month`.

### Listar snapshots disponíveis

```bash
curl https://cnpj.example.com/sources/months \
  -H "X-Import-Token: $IMPORT_API_TOKEN"
```

`GET /sources/months` retorna `source_month`, `last_modified`, `is_complete`,
`already_imported` e `status`. `source_month` é sempre o nome da pasta, como
`2026-05-10`. A data `last_modified` exibida pelo servidor é apenas informativa
e nunca identifica o snapshot.

### Importar o snapshot completo mais recente

```bash
curl -X POST https://cnpj.example.com/imports/latest \
  -H "X-Import-Token: $IMPORT_API_TOKEN"
```

O serviço ignora diretórios recentes ainda incompletos e retorna:

```json
{"run_id": 123, "status": "QUEUED"}
```

### Importar um mês específico

```bash
curl -X POST https://cnpj.example.com/imports/month/2026-05-10 \
  -H "X-Import-Token: $IMPORT_API_TOKEN"
```

O valor deve ser a data exata do diretório no formato `YYYY-MM-DD`.

Chamadas manuais não criam outra run quando já existe uma run `QUEUED` ou
`RUNNING`; nesse caso a API retorna `409 Conflict` com a run ativa.

## Modos de importação mensal

O padrão é manual:

```text
AUTO_IMPORT_ENABLED=false
```

Nesse modo, use `POST /imports/month/{source_month}` para um snapshot específico
ou `POST /imports/latest` para o mais recente completo. O app nunca percorre
meses antigos por conta própria; a primeira carga recomendada é o snapshot
completo mais recente.

Com `AUTO_IMPORT_ENABLED=true`, o scheduler verifica a origem a cada
`AUTO_IMPORT_CHECK_INTERVAL_SECONDS`, com jitter opcional. Ele apenas enfileira o
snapshot completo mais recente ainda não importado. O worker continua responsável
pela execução e usa seu próprio `WORKER_POLL_INTERVAL_SECONDS`.

O scheduler não enfileira nada quando há run `QUEUED` ou `RUNNING`. Após uma
falha automática, respeita o backoff e tenta no máximo
`AUTO_IMPORT_MAX_RETRIES_PER_MONTH`. Ao atingir o limite, somente uma chamada
manual libera novas tentativas automáticas para aquele mês.

### Acompanhar importações

```bash
curl "https://cnpj.example.com/imports/runs?limit=20&offset=0" \
  -H "X-Import-Token: $IMPORT_API_TOKEN"

curl https://cnpj.example.com/imports/runs/123 \
  -H "X-Import-Token: $IMPORT_API_TOKEN"
```

Estados finais: `SUCCEEDED` ou `FAILED`. O campo `phase`, os contadores e o
heartbeat mostram o progresso.

Enquanto uma run está `RUNNING`, uma thread separada atualiza `heartbeat_at`.
Antes de criar qualquer run, o app recupera registros órfãos. Runs sem heartbeat
por mais de `RUN_STALE_TIMEOUT_SECONDS` falham com `STALE_RUN_TIMEOUT`; jobs
`QUEUED` abandonados falham com `STALE_QUEUE_TIMEOUT`. O índice parcial global
continua garantindo apenas uma run `QUEUED`/`RUNNING`.

### Estatísticas

```bash
curl https://cnpj.example.com/stats \
  -H "X-Import-Token: $IMPORT_API_TOKEN"
```

### Limpar cache

```bash
curl -X POST https://cnpj.example.com/maintenance/cleanup-cache \
  -H "X-Import-Token: $IMPORT_API_TOKEN"
```

Diretórios mais novos que `CACHE_RETENTION_DAYS` e meses ligados a jobs
`QUEUED` ou `RUNNING` são preservados.

## Uso pelo n8n

O n8n deve apenas disparar e acompanhar o job, sem baixar ou manipular ZIPs.

1. Use um node HTTP Request com método `POST`.
2. URL: `https://cnpj.example.com/imports/latest`.
3. Header: `X-Import-Token` com o segredo armazenado nas credenciais do n8n.
4. Guarde o `run_id`.
5. Consulte `GET /imports/runs/{run_id}` periodicamente.
6. Continue o workflow somente quando o status for `SUCCEEDED`; trate `FAILED`
   como erro e encaminhe `error_message` para monitoramento.

Evite manter a requisição inicial aberta: ela retorna `202` imediatamente.

## CLI

Com as mesmas variáveis de ambiente configuradas:

```bash
python -m app.cli import-latest
python -m app.cli import-month 2026-05-10
python -m app.cli stats
python -m app.cli cleanup-cache
```

Os comandos de importação executam o job de forma síncrona e também respeitam o
advisory lock.

## Regras de atividade

Somente situação cadastral `02` ou `2` é importada como ativa. Em cada UPSERT:

- `is_active = TRUE`
- `inactive_at = NULL`
- `situacao_cadastral = 'ATIVA'`
- `last_seen_source_month = <snapshot>`

Após o snapshot inteiro concluir, CNPJs previamente gerenciados pelo importador
e não vistos no novo snapshot recebem `is_active = FALSE`. `inactive_at` é
preenchido apenas na transição. Nenhum registro de `companies` é excluído.

Se o job falhar antes da conclusão, a etapa de inativação não é executada.

## Porte e MEI

- `00` ou vazio: `NAO_INFORMADO`
- `01`: `ME`
- `03`: `EPP`
- `05`: `DEMAIS`
- opção MEI ativa no arquivo Simples: `MEI`

O campo `porte` mantém a descrição legível da Receita.

## Categorias de CNAE

Edite `config/categories.yml`. As regras são avaliadas na ordem e podem usar:

```yaml
- name: Tecnologia
  code_prefixes: ["620", "631"]
  keywords: ["software", "tecnologia da informação"]
```

Depois da alteração, faça novo deploy e reimporte um snapshot. CNAEs sem
correspondência recebem `Outros`; `categoria_sub` recebe a descrição do CNAE.

## Logs e erros

Os logs do processo são escritos em stdout e aparecem na área de logs do
EasyPanel. O banco mantém:

- `cnpj_import_runs`: status e totais do job.
- `cnpj_import_files`: download e checkpoint por ZIP.
- `cnpj_import_errors`: amostras resumidas, limitadas por configuração.

Não é armazenado JSON bruto das linhas.

## Validação no PostgreSQL

```sql
SELECT COUNT(*) FROM companies;

SELECT uf, COUNT(*)
FROM companies
GROUP BY uf
ORDER BY COUNT(*) DESC;

SELECT categoria_macro, COUNT(*)
FROM companies
GROUP BY categoria_macro
ORDER BY COUNT(*) DESC;

SELECT is_active, COUNT(*)
FROM companies
GROUP BY is_active;

SELECT *
FROM cnpj_import_runs
ORDER BY id DESC
LIMIT 10;
```

## Desenvolvimento e testes

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Os testes PostgreSQL reais são habilitados com:

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/test_cnpj \
python -m pytest tests/integration -q
```

O banco de teste deve ser descartável.
