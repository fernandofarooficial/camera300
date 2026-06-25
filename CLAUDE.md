# CLAUDE.md — Guia de Desenvolvimento

## Esquemas de Banco de Dados

Os esquemas das bases de dados usadas no projeto ficam em `doc_fonte/base_de_dados/`:

- **faciais** → `doc_fonte/base_de_dados/faciais_doc.sql`
- **microvix** → `doc_fonte/base_de_dados/microvix_doc.sql`

Sempre consulte esses arquivos ao desenvolver qualquer funcionalidade que envolva acesso ao banco de dados, para garantir que nomes de tabelas, colunas e relacionamentos estejam corretos.

---

## Arquitetura Geral

Sistema Flask de reconhecimento facial em tempo real. Fluxo principal:

```
POST /api/data/facial_recognition
  → dedup por track_id (5s)
  → Thread: salvar_rosto → get_best_face → query_heimdall
       → INSERT faciais.detection_records (se score >= SCORE_MINIMO)
       → admin_people → obter_person_id_legado → telegram_cliente_chegou
```

**Stack:** Flask + gunicorn, PostgreSQL/psycopg2 (2 pools), templates Jinja2 + SSE, Heimdall (reconhecimento facial), Telegram Bot API.

---

## Módulos principais

| Arquivo | Responsabilidade |
|---|---|
| `app.py` | Flask app, rotas principais, SSE, dedup, threading |
| `tracks.py` | Blueprint `/tracks`, Heimdall, CRUD pessoas, caixa, carga Microvix |
| `db.py` | admin_people, criar_pessoa, telegram_cliente_chegou |
| `config.py` | Pools de conexão, variáveis de ambiente, MICROVIX_PORTAIS |
| `microvix_ingest.py` | Ingestão incremental e histórica Microvix → PostgreSQL |
| `telegram.py` | enviar_mensagem_telegram |
| `tracer.py` | Log em memória por track_id, broadcast SSE |

---

## Conexões de banco

Dois pools PostgreSQL no mesmo DSN (`postgresql://fefa_dev:Fd7493dt@72.60.58.241:5432/lojas`):

- `get_faciais_conn()` / `release_faciais_conn()` → `search_path=faciais` (reconhecimento facial)
- `get_pg_conn()` / `release_pg_conn()` → `search_path=microvix` (dados Microvix/NFs)

> `get_conn()` (MySQL) ainda declarado em config.py mas **não utilizado**.

---

## Configurações (config.py)

| Variável | Padrão | Observação |
|---|---|---|
| `SCORE_MINIMO` | `0.72` | Score mínimo de reconhecimento facial |
| `FLAG_NOVO_ANONIMO` | `"C"` | Todo novo anônimo é criado como VIP (dispara Telegram) |
| `HEIMDALL_START_DATE` | `"2026-04-02"` | Configurável via env var |
| `HEIMDALL_END_DATE` | `"2026-12-31"` | Configurável via env var |
| `MICROVIX_CNPJ` | `"49104467000170"` | CNPJ do portal principal |
| `MICROVIX_CNPJ2` | `"34881719000109"` | CNPJ do segundo portal (se vazio, não adiciona) |
| `MICROVIX_PORTAIS` | lista automática | Gerada a partir dos CNPJs acima |

---

## Ingestão Microvix — Multi-CNPJ (desde commit 0039)

`microvix_ingest.py` suporta múltiplos portais Linx Microvix:

- **`MICROVIX_PORTAIS`** em `config.py`: lista de dicts `{cnpj, chave, grupo, store_id}`.
- Todas as funções de ingestão recebem `portal` (dict) como parâmetro.
- Timestamps de controle por `(metodo, cnpj_emp)` em `microvix_sync_control`.
- **`run_full_load(cnpj, data_inicio)`** → carga histórica a partir de uma data.
- **`POST /tracks/carga/full-load`** → dispara `run_full_load()` em thread background.
  - Body JSON: `{"cnpj": "...", "data_inicio": "YYYY-MM-DD"}`.
  - Valida que o CNPJ está em `MICROVIX_PORTAIS`.

---

## Tela Caixa (desde commit 0042)

Cruza notas fiscais Microvix com faces detectadas para identificar compradores.

### Lógica
1. Lista NFs do dia de uma loja (`cod_natureza_operacao='10030'`, `cancelado='N'`, `excluido='N'`, `codigo_cliente=1`).
2. Busca candidatos: pessoas do tipo `'C'` detectadas pelas câmeras da loja em janela de ±10 min por NF.
3. Exibe confirmados via tabela `faciais.person_purchases`.

### Constraint importante
`documento` **não é único** no Microvix — a mesma numeração pode existir em lojas distintas. Sempre filtrar também por `cnpj_emp` em queries a `microvix_movimento`.

### Mapeamentos carregados na inicialização (tracks.py)
- `CNPJ_STORE_MAP` → `{cnpj_str: store_id}` lido de `faciais.stores.cnpj` (coluna `int8`, sem zeros à esquerda).
- `_cnpj_key(cnpj)` → normaliza CNPJ para lookup (remove formatação e zeros à esquerda).
- `microvix.cnpj_emp` é `varchar(14)` com zeros à esquerda → usar `.zfill(14)` ao comparar.
- `CAMERA_STORE_MAP`, `STORE_NAME_MAP`, `CAMERA_STORE_NAME_MAP`, `STORE_CAMERAS_MAP` → derivados de `faciais.cameras`.

### Tabelas envolvidas
- `faciais.person_purchases` → `(person_purchase_id, person_id, store_id, bill, is_cancelled, is_identified)`. PK única por `(store_id, bill)`.
- `faciais.stores.cnpj` → CNPJ da loja como `int8` (sem zeros à esquerda).
- `faciais.detection_records.store_id` → loja onde a detecção ocorreu (coluna adicionada).

### Rotas da tela caixa
| Rota | Método | Descrição |
|---|---|---|
| `/tracks/caixa` ou `/m/tracks/caixa` | GET | Página principal; params: `store_id`, `data` (YYYY-MM-DD) |
| `/tracks/caixa/nf/<documento>/pessoa` | POST | Confirma comprador; body: `{person_id, cnpj_emp, force?}` |
| `/tracks/caixa/nf/<documento>/pessoa` | DELETE | Remove comprador confirmado |
| `/tracks/caixa/pessoa/<person_id>` | GET | Dados da pessoa |
| `/tracks/caixa/nf/<documento>` | GET | Itens da NF em JSON |

**Validação de confirmação:** ao confirmar pessoa, verifica se ela foi detectada por câmera da loja na janela ±10 min. Se não, retorna HTTP 422 com `"pode_forcar": true`; re-enviar com `force: true` bypassa.

### Templates
- `templates/tracks_caixa.html` (desktop)
- `templates/m_caixa.html` (mobile)

---

## Match por loja (db.py — desde commit 0040)

`obter_person_id_legado(track_id, store_id=None)` aceita `store_id` opcional para restringir o match de track_id a registros da mesma loja.

---

## Tela Lista — ações sobre ocorrências (`/tracks/lista`)

Cada ocorrência na faixa horizontal exibe três botões ao passar o mouse:

| Botão | Cor | Ação |
|---|---|---|
| `↗ Mover` | azul | Move o registro para uma pessoa existente (modal com busca por ID) |
| `+ Novo` | âmbar | Cria nova pessoa a partir desta imagem |
| `🗑` | vermelho | Exclui o registro individual |

### Rota `POST /tracks/api/registro/<reg_id>/nova-pessoa`

Criada em `tracks.py` (`nova_pessoa_de_registro`). Lógica:

1. Busca o `detection_record` pelo `reg_id`, obtendo `track_id` e `person_id` original.
2. Insere nova `people` com `reference_track_id = track_id` e `person_type_id = FLAG_NOVO_ANONIMO`.
3. Define `full_name = "AnônimoXXX"` e `nickname = "AXXX"` (mesmo padrão de `criar_pessoa` em `db.py`).
4. Move **apenas este registro** (`detection_record_id`) para a nova pessoa — diferente de `criar_pessoa` que move todos os registros do `track_id`.
5. Se a pessoa original ficou sem registros, remove-a automaticamente (mesmo comportamento de `mover_registro`).

Retorna: `{success, person_id, nome, pessoa_excluida}`.

---

## Flags de pessoa

| Flag | Significado |
|---|---|
| `"C"` | Cliente VIP — dispara notificação Telegram |
| `"A"` | Anônimo — aparece nos gráficos, sem Telegram |
| `"F"` | Franqueado |
| `"E"` | Empregado |
| `"K"` | Criança |
| `"P"` | Prestador |

---

## Rotas completas (Blueprint `/tracks`)

### Visualização
- `GET /tracks` → últimas 5 tracks com Heimdall
- `GET /tracks/resumo` → resumo das últimas 30 tracks
- `GET /tracks/lista` → listagem paginada de pessoas (5/pág)
- `GET /tracks/tabuleiro` → grid visual (30/pág)
- `GET /tracks/permanencia` → permanência estimada (5/pág)
- `GET /tracks/quadro` → analytics (gráficos)
- `GET /tracks/dados` → página de dados/câmeras
- `GET /tracks/snapshot/<camera_id>` → frame RTSP via OpenCV
- `GET /tracks/api` → JSON das últimas 5 tracks
- `GET /tracks/export` → página de exportação com filtro de datas
- `GET /tracks/export/download` → gera `.xlsx` (openpyxl)
- `GET /tracks/logs` → log do ZIONS_API_URL

### CRUD de pessoas
- `GET /tracks/api/pessoa/<id_unico>` → busca pessoa
- `POST /tracks/api/pessoa/<id_unico>` → atualiza dados
- `DELETE /tracks/api/pessoa/<id_unico>` → exclui pessoa e registros
- `POST /tracks/api/pessoa/<id_unico>/base` → atualizar_base_pessoa
- `DELETE /tracks/api/registro/<reg_id>` → exclui registro individual
- `POST /tracks/api/registro/<reg_id>/mover` → move registro para outro id_unico
- `POST /tracks/api/registro/<reg_id>/nova-pessoa` → cria nova pessoa a partir do registro

### Ingestão Microvix
- `GET /tracks/carga` → log das 20 últimas cargas
- `POST /tracks/carga/sync` → ingestão incremental em background
- `GET /tracks/carga/status` → status da sincronização
- `POST /tracks/carga/full-load` → carga histórica (`{cnpj, data_inicio}`)

### Outras
- `POST /api/adequar_bases` → adequar_bases() assíncrono
- `GET /api/adequar_bases/status` → status
- `GET /api/traces` → trace_entries JSON
- `GET /events` → eventos em memória
- `POST /clear` → limpa eventos e traces
- `GET /api/track_image/<track_id>` → URL da imagem do rosto
- `GET /stream` → SSE stream

---

## Bugs/issues conhecidos

- `config.py` — credenciais e token Telegram hardcoded como defaults (vazam no repositório).
- `db.py` — `criar_pessoa` usa `FLAG_NOVO_ANONIMO` (default `"C"`) → todo novo anônimo dispara Telegram.
- `tracks.py:413` — permanência estimada em 30 min quando há só 1 registro ou diferença < 2 min.
- `tracks_resumo` — threshold `0.73` hardcoded em vez de usar `SCORE_MINIMO`.
