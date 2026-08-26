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

## Deploy

```
ssh root@72.60.58.241
# no VPS: /home/workuser/camera300
git pull origin main && sudo systemctl restart camera300
```

Serviço systemd: `camera300.service`. Mesmo servidor e mesmo acesso SSH usados pelo
`retail_analytics` (e demais projetos em `/home/workuser/`) — não precisa de configuração
adicional, só trocar o diretório e o nome do serviço.

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
- `_METODOS` (`microvix_ingest.py`) hoje tem 20 entradas cobrindo 19 métodos Linx distintos (bem além das 6 tabelas originais registradas em `microvix_carga`) — inclui `microvix_vendedores`, `microvix_faturas`, `microvix_pedidos_venda`, `microvix_pedidos_compra`, `microvix_produtos_tabelas(_precos)`, `microvix_fidelidade`, `microvix_metas_vendedores`, etc. Ver `doc_microvix.sql` (pasta `db-docs/lojas`) para o schema completo dessas tabelas. `LinxFaturas` aparece em duas entradas (`microvix_faturas` e `microvix_faturas_pag`, ver seção "LinxFaturas — duas consultas" abaixo). A última entrada de `_METODOS`, `faciais_person_purchases` (→ `_sincronizar_person_purchases`), não é um método Linx — é a sincronização derivada de NFs anônimas para `faciais.person_purchases` (ver seção "Tela Caixa").
- **Cobertura parcial do Web Service:** a spec Linx Microvix (`Especificação Web Service de Saída Padrão - v176 (1).pdf`, pasta `db-docs/lojas`) documenta ~154 métodos `Linx*`; `_METODOS` só cobre os relevantes ao escopo do projeto (clientes, vendas, produtos, vendedores, faturas/pedidos). A maioria dos métodos não implementados é de verticais não usadas aqui (fiscal/tributário, ótica, e-commerce B2C, contábil, frete/NFe).

### LinxFaturas — duas consultas (emissão + pagamento)

`LinxFaturas` só aceita **um** dos dois períodos preenchido por chamada — `data_inicial`/`data_fim` (emissão) **ou** `data_inicial_pag`/`data_fim_pag` (pagamento); o par não usado precisa ir `NULL` (a API omite os campos quando ausentes do request, mesmo efeito).

- `_ingerir_faturas` → janela de **emissão** (`_data_ini(portal, 2)` a `_data_fim(portal)`), cursor de controle `LinxFaturas`. Só pega fatura nova.
- `_ingerir_faturas_pagamento` → janela de **pagamento** (`_data_ini(portal, 90)` a `_data_fim(portal)`), cursor de controle próprio `LinxFaturasPag` — **não pode reusar** o cursor de `LinxFaturas`, senão uma consulta pisa no controle de avanço da outra. Recaptura baixas (`data_baixa`/`valor_pago`) de faturas emitidas fora da janela de emissão corrente; como não dá pra saber de antemão quais faturas antigas foram pagas, roda com janela móvel generosa (90 dias) a cada sync em vez de mirar uma fatura específica.
- Ambas fazem `_upsert` na mesma tabela `microvix_faturas` por `(portal, cnpj_emp, codigo_fatura)` — a segunda consulta apenas atualiza linhas já existentes, não recria nada.
- Registrada em `_METODOS` como `("microvix_faturas_pag", _ingerir_faturas_pagamento)`, entrada separada de `("microvix_faturas", _ingerir_faturas)` — rótulo distinto na tela `/tracks/carga` (status/log), mesma tabela de destino.
- **Validado em produção em 2026-08-23**: deploy + sync manual (`POST /tracks/carga/sync`, fora do agendamento) confirmou `microvix_faturas_pag` retornando 470 registros na primeira execução, com `data_baixa`/`valor_pago` preenchidos em faturas emitidas meses antes (fora da janela de emissão corrente) que antes ficavam permanentemente sem baixa.
- **Buraco histórico (pagamentos feitos há mais de 90 dias antes da sincronização existir) fechado com backfill único** em 2026-08-23: script ad-hoc (não commitado, rodou direto no venv do VPS e foi apagado depois), reaproveitando `_chamar_api_paginado`/`_upsert`/`_max_ts` do módulo, chamando `LinxFaturas` com `timestamp=0` e `data_inicial_pag='2025-05-01'`/`data_fim_pag=hoje` pros dois portais — sem tocar no cursor `LinxFaturasPag` durante a chamada, só gravando ao final como `max(cursor atual, maior timestamp retornado)` pra não regredir o avanço normal. Resultado: portal `34881719000109` foi de 1753 pra 1797 faturas com `data_baixa` (+44, preenchendo inclusive dez/2025 e jan/2026, que antes estavam zerados); portal `49104467000170` não teve novidade (histórico de baixas já estava coberto até nov/2025 antes do backfill). Cursor `LinxFaturasPag` passou a existir pros dois portais (antes só existia pro `34881719000109`, já que o outro não tinha retornado nada nos últimos 90 dias). Esse backfill não precisa ser repetido — é coberto daqui pra frente pela janela móvel de 90 dias da sincronização incremental normal.

#### Causa raiz do "buraco" original (por que baixa sumia depois da emissão)

`_ingerir_faturas` (consulta por emissão) filtra candidatos por `data_inicial/data_fim = _data_ini(portal, 2)` — uma janela de só 2 dias antes de hoje, aplicada sobre `data_emissao`. Uma fatura só é reconsultada enquanto sua emissão está dentro desses 2 dias; passado esse prazo, nunca mais é revisitada pela sincronização normal, mesmo que `data_baixa`/`valor_pago` só sejam preenchidos no Microvix semanas/meses depois. É exatamente esse buraco que `_ingerir_faturas_pagamento` resolve daqui pra frente.

#### Achado (2026-08-23): portal 18922 sem nenhuma baixa nova desde 23/11/2025

Investigando a cobertura pós-backfill, o portal **18922** (`cnpj_emp='49104467000170'`, loja **ECOVILLE - POA - RS - IGOR**, `store_id=1`) mostrou zero faturas com `data_baixa` a partir de dezembro/2025 — nos dois tipos (`receber_pagar='R'` **e** `'P'`, não só um). Confirmado direto na API (não é limitação da nossa janela nem bug de escopo/filtro):

- Consulta `LinxFaturas` com `data_inicial_pag`/`data_fim_pag` em janelas estreitas (`2025-12-01→2026-01-31` e `2026-06-01→2026-08-23`) retornou **0 registros** pra esse portal — API respondeu vazio mesmo pedindo diretamente por esses períodos.
- No banco, distribuição de `data_baixa` (mês do pagamento) pro portal 18922: `2025-03` (3.428 R + 291 P, carga histórica), `2025-04` (147 R + 9 P, cauda), `2025-06` (48 P, capturado pelo sync novo), `2025-11` (46 R, idem) — **nada depois disso**. Última baixa registrada: **2025-11-23**.
- Comparação: o outro portal (`19926` / `34881719000109`, loja Ecoville Itapema) segue normal, com baixa até 21/08/2026 (1.456 faturas baixadas só depois de dez/2025).

**Conclusão:** não é algo corrigível em `microvix_ingest.py` — a API já é consultada corretamente e o dado simplesmente não existe na origem pra esse portal desde então. Precisa ser verificado com quem opera o Microvix da loja POA/IGOR: se ela parou de registrar baixa de faturas nesse módulo (mudou de fluxo/ferramenta de cobrança) ou se há algum problema de configuração/integração específico dessa unidade desde final de novembro/2025.

Achado à parte, sem relação direta com o gap acima: pro portal **19926** (`34881719000109`), a consulta por pagamento retorna só faturas `P` — nenhuma `R` aparece nunca (0 de 11.762 faturas a receber desse portal têm `data_baixa`, em toda a história). Também confirmado como comportamento da API (não filtro nosso): resposta bruta de `LinxFaturas` na janela de pagamento veio 100% `P` (1796/1796). Hipótese: essa loja liquida recebíveis/crediário por outro módulo do Microvix, cuja baixa não é refletida em `LinxFaturas.data_baixa` pra tipo `R`.

### Sincronização faciais.sellers ← microvix_vendedores

`_ingerir_vendedores` (método `LinxVendedores`), a cada chamada, também sincroniza `faciais.sellers` via `_sincronizar_sellers(store_id, registros)`, usando os registros já retornados pela API para aquele portal — não uma nova query no banco. Motivo: `microvix_vendedores` é chaveada por `(portal, cod_vendedor)`, mas o `portal` numérico do Microvix pode ser compartilhado entre lojas da mesma rede/grupo e a tabela não tem `cnpj_emp`; então o `store_id` correto só é conhecido no momento da chamada (escopada por `cnpjEmp=portal["cnpj"]`), não é recuperável depois via SQL. `faciais.sellers` usa `(store_id, cod_vendedor)` (constraint `uq_sellers_store_cod`), não `(portal, cod_vendedor)`.

- `seller_name` é gravado com `nome_vendedor.capitalize()` — só a primeira letra da string maiúscula, todo o resto minúsculo (não é title-case por palavra).
- `is_active` = `ativo == 'S'` **e** `data_saida` vazio/nulo (reflete os dois campos do Microvix, conforme comentário da coluna em `doc_faciais.sql`).
- Roda em todo sync (incremental agendado e full-load), pois é acionado dentro de `_ingerir_vendedores`, não como entrada separada em `_METODOS`.
- **Validado em produção em 2026-08-15**: reset manual do checkpoint `LinxVendedores` (`microvix_sync_control.last_timestamp = 0`) forçou reprocessar todos os vendedores; `faciais.sellers` populado com 30 linhas (14 na loja 1, 16 na loja 2), nomes corretamente capitalizados e `is_active` variando conforme esperado.
- **Não filtra por `tipo_vendedor`** — `microvix_vendedores` traz junto códigos que não são vendedores humanos (ex.: `"Sistema"`, `"Depósito"`, `"Vendedor loja"`, `"Ecoville"`), que acabam sincronizados em `faciais.sellers` como se fossem vendedores. Se isso for indesejado (ex.: para alocação de metas por vendedor), considerar filtrar por `tipo_vendedor IN ('V', 'A')` em `_sincronizar_sellers`.
- `seller_name` preserva artefatos de formatação do cadastro Microvix (ex.: espaço duplo) — só `.strip()` nas pontas, sem normalizar espaços internos.

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

## Tela Clientes (`/tracks/clientes`)

Ordem de chegada dos clientes do dia atual, com dados cadastrais, histórico de visitas e
últimas compras. Função `tracks_clientes` em `tracks.py`.

### Lógica
1. **Chegada** = primeira detecção facial do dia (`DISTINCT ON (person_id)`, `ORDER BY created_at ASC`),
   restrita a `person_type_id = 'C'`. Recapturas no mesmo dia não alteram a chegada já registrada
   (a query sempre pega a menor `created_at` do dia, independente de quantas vezes a pessoa for
   detectada depois).
2. Lista ordenada por chegada **decrescente** (mais recente primeiro).
3. Filtro de loja (`?store=<store_id>`, `0`/ausente = todas as lojas) — mesmo padrão de `tracks_lista`.
4. Dados cadastrais (nome, apelido, doc, idade, gênero, telefone, e-mail, flag, notas) sempre exibidos.
5. **Recorrência**: conta dias distintos com detecção **antes de hoje** (qualquer loja, não só a
   filtrada) via `ARRAY_AGG(DISTINCT date(created_at))` + `COUNT DISTINCT`. Cliente com 1+ visita
   anterior é "recorrente" e tem as datas dessas visitas listadas.
6. **Últimas compras** (só para recorrentes): até 5 dias mais recentes de compra, obtidos via
   `person_purchases` → `stores` (para o CNPJ) → `faciais.mv_microvix_vendas` — mesma materialized
   view usada em `vw_customer_ranking`, já filtrada para vendas PF válidas por loja
   (`store_serie_rules`). Histórico de compras não é limitado pela loja filtrada na tela.
7. **Produtos e quantidades por compra**: para cada um dos dias de compra exibidos, busca os itens
   das NFs daquele dia direto em `microvix.microvix_movimento` (mesmos filtros de venda válida de
   `mv_microvix_vendas`: `cod_natureza_operacao='10030'`, não cancelado/excluído, `soma_relatorio='S'`,
   `tipo_transacao` em `(P,V,S)` ou nulo), com `LEFT JOIN microvix_produtos` (por `portal, cod_produto`)
   para o nome do produto. Quando há mais de uma NF no mesmo dia, quantidades do mesmo produto são
   somadas. Consulta usa `JOIN (VALUES (...), ...) AS v(cnpj_emp, documento, data_documento, store_id)`
   para casar as compras já identificadas — evita re-filtrar por natureza/série do zero e mantém a
   correspondência 1:1 com as compras exibidas.

   **`documento` não é uma chave confiável — nem com `cnpj_emp`.** Cada série do Microvix tem sua
   própria numeração sequencial e elas se sobrepõem constantemente: o mesmo `(cnpj_emp, documento)`
   aparece em NFs completamente diferentes emitidas em datas distintas, e em casos raríssimos até na
   mesma data (série diferente). Investigação em 2026-08-26 (motivada por um relato de valor/produtos
   incompatíveis numa compra da tela Clientes) mediu o problema: **7449 pares `(cnpj_emp, documento)`**
   com mais de uma data válida no total (ambos os CNPJs — 1166 no `49104467000170`, o resto no
   `34881719000109`), e **153 das 771 compras confirmadas em `person_purchases`** (≈20%, todas no
   `34881719000109`/Ecoville Itapema) caem nessa ambiguidade. Um bug inicial na busca de itens casava
   só por `(cnpj_emp, documento)` e por isso misturava itens de uma NF vizinha com o mesmo número
   numa data diferente (ex.: pessoa com compra de R$8,00/2 itens exibindo 6 itens de duas NFs
   distintas). Corrigido em duas camadas, replicando exatamente o critério que `mv_microvix_vendas`
   já usa para calcular o valor:
   - chave de correspondência inclui `data_documento::date` (como `mv_microvix_vendas` já agrupa por
     `(cnpj_emp, documento, data_documento::date)`);
   - `JOIN faciais.store_serie_rules` exigindo `person_kind='PF'` e `serie = mm.serie` da loja —
     fecha os ~5 casos raros de mesma data com série diferente, que a chave de data sozinha não
     resolveria.

   **Achado relacionado, não corrigido (fora do escopo desta tela):** `POST /tracks/caixa/nf/<documento>/pessoa`
   (`tracks_caixa_set_pessoa`, seção "Tela Caixa") busca a NF só por `documento` (+ `cnpj_emp` quando
   informado pelo front) com `LIMIT 1`, sem desambiguar por data — nos casos de documento ambíguo,
   pode confirmar o comprador contra a NF errada (arbitrária, a que o `LIMIT 1` retornar). Mesma causa
   raiz desta seção, mas correção não aplicada aqui — avaliar separadamente se/como desambiguar
   também na confirmação de comprador.

### Edição de pessoa
Reaproveita **o mesmo modal e a mesma rota** de `tracks_lista.html`/`atualizar_pessoa`
(`POST /tracks/api/pessoa/<id_unico>`) — nenhum backend novo de edição foi criado para esta tela.

### Templates
`templates/tracks_clientes.html` — usado tanto por `/tracks/clientes` quanto por `/m/tracks/clientes`
(diferente de Lista/Caixa, não há template mobile separado; a página é responsiva via media query).

### Refresh
Sem SSE — a página tem um `setInterval` de 30s que dá `location.reload()` (refaz todas as queries
do zero, preservando `?store=` da URL atual), pausado enquanto o modal de edição de pessoa está
aberto (`modal-overlay.classList.contains('open')`) para não descartar o que o usuário está
digitando. O link "↻ Atualizar" no nav e o reload após salvar edição (`salvarPessoa`) continuam
existindo como antes, mas não são mais a única forma de ver chegadas novas.

### Escopo do filtro "cliente"
A tela só considera `person_type_id = 'C'` (mesma flag de "Cliente" usada em Caixa/Ranking) —
pessoas com outras flags (Anônimo, Franqueado, Empregado, etc.) não aparecem, mesmo que tenham
sido detectadas hoje.

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

## Formulário de edição de pessoa (telefone/e-mail)

O modal "Editar Pessoa" está **duplicado em três templates** (mesmo HTML/JS, sem componente
compartilhado): `tracks_lista.html`, `tracks_tabuleiro.html` e `tracks_permanencia.html`. Uma
alteração no formulário precisa ser replicada nos três. Campos atuais: Nome, Apelido, Idade,
Doc (CPF/CNPJ), Telefone, E-mail, Gênero, Flag, Notas.

- Colunas `faciais.people.phone` (`varchar(20)`) e `faciais.people.email` (`varchar(255)`) —
  já existiam no schema antes do formulário expô-las.
- `_CAMPO_MAP` (`tracks.py`) mapeia os nomes de campo do frontend (legado) para as colunas:
  `telefone` → `phone`, `email` → `email` (esse já bate 1:1). Usado por
  `POST /tracks/api/pessoa/<id_unico>` (`atualizar_pessoa`).
- As queries de `tracks_lista`, `tracks_tabuleiro`, `tracks_permanencia` e `buscar_pessoa`
  (`tracks.py`) trazem `p.phone AS telefone` e `p.email AS email`, propagados nos `data-*` dos
  cards/botões de cada template.

### Máscara de telefone `(99) 9-9999-9999`

Cada um dos três templates define sua própria função JS `formatTelefone(value)` (duplicada,
mesmo motivo do modal) que:
- Remove tudo que não é dígito e limita a 11 dígitos (DDD + 9 dígitos do celular).
- Formata progressivamente enquanto o usuário digita, via `oninput` no `#f-telefone`.
- É reaplicada em `abrirModal()` ao carregar o valor existente (`formatTelefone(btn.dataset.telefone)`),
  então um telefone já salvo sem formatação (ou com formatação antiga) é normalizado ao abrir o modal.
- O valor salvo no banco é a string já formatada (com parênteses/traço), não os dígitos crus —
  mesmo padrão do campo Doc, que também não normaliza antes de salvar.

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
- `GET /tracks/clientes` ou `/m/tracks/clientes` → ordem de chegada dos clientes do dia, com visitas e compras (ver seção "Tela Clientes")
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
- Dado (não bug de código): portal `18922` (loja POA/IGOR) sem nenhuma `data_baixa` nova em `microvix_faturas` desde 2025-11-23, confirmado na origem (API). Portal `19926` (Itapema) nunca recebe baixa pra faturas `receber_pagar='R'`, só `'P'`. Ver seção "LinxFaturas — duas consultas" para detalhes.

---

## Correção: queda de conexão SSL no microvix_ingest (2026-06-25)

**Sintoma:** `psycopg2.OperationalError: SSL connection has been closed unexpectedly` seguido de `psycopg2.InterfaceError: connection already closed` no rollback — abortava toda a sincronização.

**Causa:** `_run_portais` mantinha uma única `pg_conn` aberta por toda a sync (podendo durar 15+ min). O servidor PostgreSQL derruba sessões SSL ociosas. Quando um método falhava e o código tentava `pg_conn.rollback()` na conexão morta, gerava segunda exceção em cascata.

**Fix em `_run_portais` (`microvix_ingest.py`):** o `rollback()` agora está dentro de try/except; se falhar com `OperationalError` ou `InterfaceError`, descarta a conexão morta, pega uma nova do pool e continua os métodos restantes.

**Fix em `_sincronizar_person_purchases`:** o `faciais_conn.rollback()` e o `cur.close()` do bloco finally também foram protegidos com try/except, evitando exceção secundária quando essas conexões também caem.
