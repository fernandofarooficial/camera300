# CLAUDE.md — Guia de Desenvolvimento

## Esquemas de Banco de Dados

**Referência definitiva do schema:** `C:\Users\ferna\db-docs\lojas\doc_faciais.sql` e
`doc_microvix.sql` — pasta **compartilhada entre todas as aplicações locais que usam o banco
`lojas`** (`72.60.58.241:5432`), não só este projeto. Ver
`C:\Users\ferna\db-docs\lojas\README.md` para como foi gerada e como manter atualizada.

Sempre consulte esses arquivos ao desenvolver qualquer funcionalidade que envolva acesso ao
banco de dados, para garantir que nomes de tabelas, colunas e relacionamentos estejam corretos.
Comentários de coluna de `doc_faciais.sql` vêm de `COMMENT ON` já existentes no banco; os de
`doc_microvix.sql` vêm da especificação oficial do Web Service Microvix (PDF na mesma pasta
compartilhada), já que o schema `microvix` (dados sincronizados do ERP por este projeto via
`microvix_ingest.py`) não tem `COMMENT ON` no banco.

(A referência antiga `doc_fonte/base_de_dados/` não existe mais neste projeto — apontava pra
uma pasta que nunca chegou a ser criada aqui.)

---

## Arquitetura Geral

Sistema Flask de reconhecimento facial em tempo real. Fluxo principal:

```
POST /api/data/facial_recognition
  → se data.full_name != "Pessoa desconhecida": INSERT faciais.zions_identified_records e para (não processa)
  → INSERT faciais.json_records (payload bruto, image_base64 redigido)
  → dedup por track_id (5s, `DEDUP_SECONDS`)
  → Thread: salvar_rosto → query_heimdall (até 3 tentativas: `_RETRY_DELAYS = [3, 12, 20]`s) → get_best_face
       → INSERT faciais.detection_records (se score >= SCORE_MINIMO; senão desiste e marca `_processed_no_face`)
       → admin_people → obter_person_id_legado → telegram_cliente_chegou
```

### Pessoas já identificadas pelo ZIONS (desde commit 0054)

O ZIONS envia `data.full_name` no payload; quando o próprio analítico já reconheceu a pessoa por nome (valor diferente do literal `"Pessoa desconhecida"`, **inclusive quando o campo vem ausente/nulo**), o evento **não** entra no pipeline de reconhecimento facial próprio: é gravado em `faciais.zions_identified_records` (payload bruto com `image_base64` redigido, mesma redação de `json_records`) e a request retorna 200 imediatamente — sem `INSERT` em `json_records`/`detection_records`, sem dedup, sem thread `salvar_rosto`. Só quando `data.full_name == "Pessoa desconhecida"` o fluxo normal (acima) é seguido.

`zions_identified_records` não é consumida pelo app — consulta é feita direto no banco. Helper `_redact_image_base64(payload)` em `app.py` centraliza a redação do `image_base64` (raiz e `data.image_base64`).

**Stack:** Flask + gunicorn, PostgreSQL/psycopg2 (2 pools), templates Jinja2 + SSE, Heimdall (reconhecimento facial), Telegram Bot API.

### Deploy atrás de proxy reverso (`/camera300`)

Em produção a aplicação fica atrás de um proxy que expõe tudo sob o prefixo `/camera300` (ex.: `/camera300/tracks/lista`, `/camera300/stream`). As rotas Flask **não** têm esse prefixo — ele é hardcoded manualmente em todos os `fetch`/`href` dos templates (`index.html`, `m_index.html`, `tracks_lista.html`, `m_caixa.html`, `m_lista.html` etc.), não usa `url_for`. **Ao criar uma rota nova consumida por JS/links, lembre de prefixar `/camera300` no template.**

---

## Módulos principais

| Arquivo | Responsabilidade |
|---|---|
| `app.py` | Flask app, rotas principais, SSE, dedup, threading |
| `tracks.py` | Blueprint `/tracks`, Heimdall, CRUD pessoas, caixa, carga Microvix |
| `db.py` | admin_people, criar_pessoa, telegram_cliente_chegou |
| `config.py` | Pools de conexão, variáveis de ambiente, MICROVIX_PORTAIS |
| `microvix_ingest.py` | Ingestão incremental e histórica Microvix → PostgreSQL, acionada pelas rotas `/tracks/carga/*` |
| `microvix_ingest_full.py` | Script standalone (CLI, `python microvix_ingest_full.py`) de carga completa Microvix — **não** é importado pelo Flask app nem por nenhuma rota; execução manual |
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

> `HEATMAP_API_URL`, `HEATMAP_API_BASE`, `HEATMAP_AUTH` (Basic Auth) **não** estão em `config.py` — ficam hardcoded no topo de `tracks.py` (linhas ~19-21), diferente do padrão do resto do projeto de usar env vars em `config.py`.

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
- `_METODOS` (`microvix_ingest.py`) hoje cobre 19 métodos Linx (bem além das 6 tabelas originais registradas em `microvix_carga`) — inclui `microvix_vendedores`, `microvix_faturas`, `microvix_pedidos_venda`, `microvix_pedidos_compra`, `microvix_produtos_tabelas(_precos)`, `microvix_fidelidade`, `microvix_metas_vendedores`, etc. Ver `doc_microvix.sql` (pasta `db-docs/lojas`) para o schema completo dessas tabelas. A última entrada de `_METODOS`, `faciais_person_purchases` (→ `_sincronizar_person_purchases`), não é um método Linx — é a sincronização derivada de NFs anônimas para `faciais.person_purchases` (ver seção "Tela Caixa").

---

## Tela Caixa (desde commit 0042)

Cruza notas fiscais Microvix com faces detectadas para identificar compradores.

### Lógica
1. Lista NFs do dia de uma loja (`cod_natureza_operacao='10030'`, `cancelado='N'`, `excluido='N'`, `codigo_cliente=1`, `tipo_transacao IN ('P','V')` ou nulo).
2. Busca candidatos: pessoas do tipo `'C'` detectadas pelas câmeras da loja em janela de ±10 min por NF.
3. Exibe confirmados via tabela `faciais.person_purchases`.

### Constraint importante
`documento` **não é único** no Microvix — a mesma numeração pode existir em lojas distintas. Sempre filtrar também por `cnpj_emp` em queries a `microvix_movimento`.

### Mapeamentos carregados na inicialização (tracks.py)
- `CNPJ_STORE_MAP` → `{cnpj_str: store_id}` lido de `faciais.stores.cnpj` (coluna `int8`, sem zeros à esquerda).
- `_cnpj_key(cnpj)` → normaliza CNPJ para lookup no `CNPJ_STORE_MAP` (remove formatação e zeros à esquerda, `int()` round-trip). Usar sempre que comparar um `cnpj_emp` do Microvix contra `CNPJ_STORE_MAP`.
- `microvix.cnpj_emp` é `varchar(14)` com zeros à esquerda → usar `.zfill(14)` ao montar o filtro de query (ex.: `cnpj_sel_padded` em `tracks_caixa`).
- `CAMERA_STORE_MAP`, `STORE_NAME_MAP`, `CAMERA_STORE_NAME_MAP`, `STORE_CAMERAS_MAP` → derivados de `faciais.cameras`.

### CNPJ na confirmação de comprador (desde commit 0053)
`POST /tracks/caixa/nf/<documento>/pessoa` agora recebe `cnpj_emp` no body (enviado pelo front, que já sabe qual loja/CNPJ está exibindo). Se presente, filtra a NF por `documento + cnpj_emp` no lugar de só `documento` — essencial porque `documento` se repete entre lojas. Sem `cnpj_emp` no body, cai no comportamento antigo (primeira NF encontrada por `documento`, `LIMIT 1`).

### Tabelas envolvidas
- `faciais.person_purchases` → `(person_purchase_id, person_id, store_id, bill, is_cancelled, is_identified)`. PK única por `(store_id, bill)`.
- `faciais.stores.cnpj` → CNPJ da loja como `int8` (sem zeros à esquerda).
- `faciais.detection_records.store_id` → loja onde a detecção ocorreu (coluna adicionada).

### Rotas da tela caixa
| Rota | Método | Descrição |
|---|---|---|
| `/tracks/caixa` ou `/m/tracks/caixa` | GET | Página principal; params: `store_id`, `data` (YYYY-MM-DD) |
| `/tracks/caixa/nf/<documento>/pessoa` ou `/m/...` | POST | Confirma comprador; body: `{person_id, cnpj_emp, force?}` |
| `/tracks/caixa/nf/<documento>/pessoa` ou `/m/...` | DELETE | Remove comprador confirmado |
| `/tracks/caixa/pessoa/<person_id>` | GET | Dados da pessoa |
| `/tracks/caixa/nf/<documento>` | GET | Itens da NF em JSON |
| `/tracks/api/empresas` | GET | Empresas + cor de tema (ver seção "Tema por empresa") |

**Validação de confirmação:** ao confirmar pessoa, verifica se ela foi detectada por câmera da loja na janela ±10 min. Se não, retorna HTTP 422 com `"pode_forcar": true`; re-enviar com `force: true` bypassa.

### Templates
- `templates/tracks_caixa.html` (desktop)
- `templates/m_caixa.html` (mobile)

---

## Match por loja (db.py — desde commit 0040)

`obter_person_id_legado(track_id, store_id=None)` aceita `store_id` opcional para restringir o match de track_id a registros da mesma loja.

---

## Mapa de Calor (`/tracks/heatmap`, desde commit ~0019)

Consulta uma API externa de heatmap (serviço separado, não é o Heimdall):

- `HEATMAP_API_URL` (`POST .../api/heatmap`) → recebe `{camera_id, data_ini, data_fim}`, retorna `{ok, resultado}` com URLs de imagens.
- `_prefixar_urls_heatmap(resultado)` reescreve as URLs da resposta para passarem pelo proxy `/tracks/heatmap/img` (evita expor a API externa e suas credenciais direto ao browser).
- `GET /tracks/heatmap/img?url=...` → proxy autenticado (Basic Auth); só aceita `url` que comece com `HEATMAP_API_BASE`, senão 403.

---

## Tema por empresa (`/tracks/api/empresas`, desde commit 0032)

Retorna `{company_id, company_name, background_color}` de `faciais.companies` LEFT JOIN `faciais.company_themes`, usado pelo front (ex. `m_caixa.html`) para colorir a UI conforme a empresa/loja. Cor default `#0f1117` quando não há tema cadastrado.

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
- `GET /tracks/lista` ou `/m/tracks/lista` → listagem paginada de pessoas (5/pág)
- `GET /tracks/tabuleiro` ou `/m/tracks/tabuleiro` → grid visual (30/pág)
- `GET /tracks/permanencia` → permanência estimada (5/pág)
- `GET /tracks/quadro` ou `/m/tracks/quadro` → analytics (gráficos)
- `GET /tracks/dados` → página de dados/câmeras
- `GET /tracks/heatmap` → página do mapa de calor (params: `camera_id`, `data_ini`, `data_fim`)
- `GET /tracks/heatmap/img` → proxy autenticado de imagens do heatmap (param `url`)
- `GET /tracks/snapshot/<camera_id>` → frame RTSP via OpenCV
- `GET /tracks/api` → JSON das últimas 5 tracks
- `GET /tracks/api/empresas` → lista empresas + cor de tema (`companies` + `company_themes`)
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

### Tela Caixa (detalhes na seção "Tela Caixa" acima)
- `GET /tracks/caixa` ou `/m/tracks/caixa` → página principal
- `POST /tracks/caixa/nf/<documento>/pessoa` ou `/m/...` → confirma comprador
- `DELETE /tracks/caixa/nf/<documento>/pessoa` ou `/m/...` → remove comprador confirmado
- `GET /tracks/caixa/nf/<documento>` → itens da NF em JSON
- `GET /tracks/caixa/pessoa/<person_id>` → dados da pessoa (apenas `person_type_id='C'`)

### Outras (app.py, fora do blueprint `/tracks`)
- `GET /` → `index.html` (dashboard desktop)
- `GET /m/` → `m_index.html` (dashboard mobile)
- `GET /service-worker.js` → service worker do PWA (mobile)
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
- `tracks.py:576` (`tracks_permanencia`) — permanência estimada em 30 min quando há só 1 registro ou diferença < 2 min (linha pode se mover; buscar por `estimado = True`).
- `tracks_resumo` — threshold `0.73` hardcoded em vez de usar `SCORE_MINIMO`.

---

## Correção: queda de conexão SSL no microvix_ingest (2026-06-25)

**Sintoma:** `psycopg2.OperationalError: SSL connection has been closed unexpectedly` seguido de `psycopg2.InterfaceError: connection already closed` no rollback — abortava toda a sincronização.

**Causa:** `_run_portais` mantinha uma única `pg_conn` aberta por toda a sync (podendo durar 15+ min). O servidor PostgreSQL derruba sessões SSL ociosas. Quando um método falhava e o código tentava `pg_conn.rollback()` na conexão morta, gerava segunda exceção em cascata.

**Fix em `_run_portais` (`microvix_ingest.py`):** o `rollback()` agora está dentro de try/except; se falhar com `OperationalError` ou `InterfaceError`, descarta a conexão morta, pega uma nova do pool e continua os métodos restantes.

**Fix em `_sincronizar_person_purchases`:** o `faciais_conn.rollback()` e o `cur.close()` do bloco finally também foram protegidos com try/except, evitando exceção secundária quando essas conexões também caem.
