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
ssh camwatch-vps          # alias SSH do VPS 72.60.58.241 (usar o alias, não root@IP)
# no VPS: /home/workuser/camera300
git pull origin main
# depois, restart com espera (ver abaixo)
```

### Restart com espera até o serviço responder

Em vez de só `systemctl restart`, usar o loop abaixo: reinicia e fica sondando a porta local do
gunicorn (`127.0.0.1:5001`, sem passar pelo proxy) até ela responder, com timeout de 3 min. Cada
sonda usa `curl -m 2` (ver aviso de `--max-time` abaixo), então não prende thread do worker.

```bash
systemctl restart camera300; s=$(date +%s); ok=1
until curl -s -m 2 -o /dev/null http://127.0.0.1:5001/; do
  sleep 1
  [ $(( $(date +%s) - s )) -gt 180 ] && { ok=0; break; }
done
[ $ok = 1 ] && echo "Pronto em $(( $(date +%s) - s ))s" || echo "Não subiu em 3 min — ver journalctl -u camera300 -n 100"
```

`curl -s` sem `-f` considera "no ar" qualquer resposta HTTP (inclusive 500) — o objetivo é só saber
que o worker já está aceitando conexões, não validar uma rota específica. Quando responde, o import
de `tracks.py` (incluindo os retries de `CNPJ_STORE_MAP`/`_carregar_cameras()`, até ~35s) já terminou.

**Se o loop der "Não subiu em 3 min" sem erro nenhum no `journalctl`:** checar se o log tem
`Booting worker with pid: ...` depois do `Control socket listening`. Em 2026-10-24 o worker
(processo filho do gunicorn, 1 thread só, ~20 MB RSS) travou logo após o fork em `futex_wait_queue`
(`cat /proc/<pid>/wchan`), antes até de importar o `app` — sem conexão com o banco, sem linha de log,
porta escutando com fila (`ss -ltnp | grep 5001`, `Recv-Q` crescendo). Sintoma típico de deadlock
pós-fork (gunicorn 25 tem uma thread de control socket no master), intermitente e sem relação com o
código deployado: um segundo `systemctl restart camera300` resolveu em 1s. Repetiu em 2 de ~7
restarts no mesmo dia.

**Mitigação aplicada (2026-10-24):** `--no-control-socket` adicionado ao `ExecStart` do
`camera300.service` no VPS (backup do unit anterior em `/etc/systemd/system/camera300.service.bak-*`)
— o master passou de 2 threads pra 1 e o `gunicorn.ctl` deixou de ser criado. O projeto não usa o
control socket (CLI de controle do gunicorn), então não há perda. Causa não confirmada por stack
trace (hipótese forte: fork com lock segurado pela thread do control socket); se o travamento voltar
mesmo assim, capturar a pilha do worker preso com `py-spy dump --pid <pid>` antes de reiniciar. O
`ExecStartPre=/bin/rm -f .../gunicorn.ctl` do unit ficou inofensivo (arquivo não existe mais).

Serviço systemd: `camera300.service`. Mesmo servidor e mesmo acesso SSH usados pelo
`retail_analytics` (e demais projetos em `/home/workuser/`) — não precisa de configuração
adicional, só trocar o diretório e o nome do serviço.

**Cuidado ao validar com `curl` logo após o restart:** gunicorn roda com `--workers 1 --worker-class
gthread --threads 4` — só 4 threads no total pro processo inteiro. Um `curl` sem `--max-time` (ou
abortado no meio, ex. Ctrl+C) pode deixar uma thread do worker presa numa escrita bloqueante pra um
socket que não vai mais ser lido, e ela só libera depois do timeout de retransmissão do TCP (minutos).
Repetir esse erro poucas vezes esgota as 4 threads e trava o serviço inteiro — **para qualquer rota**,
não só a testada, dando a falsa impressão de que o deploy quebrou algo (aconteceu em 2026-08-26: um
`curl` de verificação sem timeout travou `/tracks/caixa`, e tentativas seguintes de diagnóstico
travaram até `/tracks/lista`, que não tinha relação nenhuma com a mudança). Sempre usar
`curl --max-time <N>` ao validar deploy, e se acontecer, um `systemctl restart camera300` limpo
resolve — não é preciso reverter o código.

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

## Fuso horário (America/Sao_Paulo)

O VPS e o Postgres rodam em **UTC**, mas os dois pools de `config.py` abrem a sessão com
`options="-c timezone=America/Sao_Paulo"` — então `now()`/`CURRENT_DATE` nas queries da aplicação, e
por consequência todo `created_at` gravado por ela (ex.: `detection_records`, `DEFAULT now()`), estão
em **horário de Brasília** (`timestamp` sem fuso). Conexões abertas fora dos pools (ex.: `psycopg2`
avulso pra diagnóstico) caem em UTC — cuidado ao comparar `now()` com `created_at` nelas.

Em Python, **nunca usar `date.today()`/`datetime.now()`** pra "hoje"/"agora": no servidor UTC eles
viram o dia seguinte a partir das 21h locais (bug corrigido em 2026-10-24 — telas como Clientes,
Caixa e Anomalias abriam vazias no fim do dia, e as janelas de data da sincronização Microvix
começavam "amanhã"). Usar `hoje_local()` / `agora_local()` de `config.py` (`agora_local()` devolve
datetime sem tzinfo, mesmo formato dos timestamps do banco). `microvix_ingest_full.py` (CLI
standalone, não importa `config`) tem o próprio `_hoje_local()`. `tracer.py` usa offset fixo `-3`
(`_TZ_SP`) só pra exibir hora no log — equivalente enquanto o Brasil não tiver horário de verão.

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
1. Lista NFs do dia de uma loja (`cod_natureza_operacao='10030'`, `cancelado='N'`, `excluido='N'`, `codigo_cliente=1`, `tipo_transacao <> 'J'` ou nulo — `'J'` = Ajuste de Estoque, não é venda; todos os demais tipos são considerados faturamento).
2. Busca candidatos: pessoas do tipo `'C'` detectadas pelas câmeras da loja em janela de ±10 min por NF.
3. Exibe confirmados via tabela `faciais.person_purchases`.

### Critério de faturamento — `tipo_transacao` (alterado em 2026-09-07)
Único filtro de exclusão em `microvix_movimento.tipo_transacao`: **apenas `'J'` (Ajuste de
Estoque) é desconsiderado** — todos os demais tipos (inclusive nulo) contam como faturamento.
Antes, cada consulta restringia por uma allowlist diferente e inconsistente entre si (`= 'V'` em
alguns lugares, `IN ('P','V')` em outros, `= ANY (ARRAY['P','V','S'])` em outro ainda), deixando de
fora tipos de transação válidos que não estavam explicitamente listados. Padronizado em todo o
projeto (`tracks.py`, `microvix_ingest.py`) como `(tipo_transacao <> 'J' OR tipo_transacao IS
NULL)` — mesmo critério já usado pela materialized view `faciais.mv_microvix_vendas` (ver
`doc_faciais.sql`, comentário da view; migration `filtro_tipo_transacao_diferente_j.sql` no
`retail_analytics`).

### Definição de pessoa física — `tipo_cliente` (alterado em 2026-09-07)
Classificação de venda PF (pessoa física, usada na Tela Clientes — item 6 — e em
`faciais.mv_microvix_vendas`) passou a ser feita por
`microvix.microvix_clientes_fornecedores.tipo_cliente` (`'F'` = física, `'J'` = jurídica; PK
`(portal, cod_cliente)`), casada com `microvix_movimento` por `(mm.portal, mm.codigo_cliente)`.
Critério: `cf.tipo_cliente IS NULL OR cf.tipo_cliente = 'F'` (cliente sem cadastro em
`microvix_clientes_fornecedores`, ex. venda ao consumidor sem CPF na nota, ainda conta como PF).

Antes, a classificação era feita por **série** da loja, via tabela `faciais.store_serie_rules`
(`person_kind='PF'` e `serie = mm.serie`) — tabela **removida** do schema em 2026-09 pela migration
`descontinuar_store_serie_rules.sql` no `retail_analytics`. A troca elimina a dependência de
`store_id` nessa classificação (`store_serie_rules` era escopada por loja; `tipo_cliente` é uma
propriedade do cadastro do cliente/portal, não da loja).

### Constraint importante: documento não é único — nem com cnpj_emp
`documento` **não é único** no Microvix: além de poder existir em lojas distintas (`cnpj_emp`
diferente), a mesma numeração se repete entre **séries diferentes da mesma loja** — cada série tem
sua própria numeração sequencial, e é isso (não a data) que causa o reaproveitamento. A
chave que de fato identifica uma NF é **`(cnpj_emp, serie, documento)`**. Sempre filtrar pelos três
campos em queries a `microvix_movimento` que precisam achar UMA nota específica (itens, confirmação
de comprador) — filtrar só por `cnpj_emp` (sem `serie`) ainda deixa ambiguidade.

Medido em 2026-08-26 (investigação motivada por um relato na tela Clientes, ver seção "Tela
Clientes"): `(cnpj_emp, documento)` sozinho tem milhares de pares com mais de uma NF; restringindo a
`(cnpj_emp, serie, documento)` cai pra **7 pares residuais** — todos numa anomalia pontual de dados
de 2024 num único CNPJ/série (não o padrão sistêmico, não compensa tentar fechar via query).

### Mapeamentos carregados na inicialização (tracks.py)
- `CNPJ_STORE_MAP` → `{cnpj_str: store_id}` lido de `faciais.stores.cnpj` (coluna `int8`, sem zeros à esquerda).
- `_cnpj_key(cnpj)` → normaliza CNPJ para lookup no `CNPJ_STORE_MAP` (remove formatação e zeros à esquerda, `int()` round-trip). Usar sempre que comparar um `cnpj_emp` do Microvix contra `CNPJ_STORE_MAP`.
- `microvix.cnpj_emp` é `varchar(14)` com zeros à esquerda → usar `.zfill(14)` ao montar o filtro de query (ex.: `cnpj_sel_padded` em `tracks_caixa`).
- `CAMERA_STORE_MAP`, `STORE_NAME_MAP`, `CAMERA_STORE_NAME_MAP`, `STORE_CAMERAS_MAP` → derivados de `faciais.cameras` (via `_carregar_cameras()`).
- Todas as cargas acima rodam com retry (`_BOOT_QUERY_RETRY_DELAYS`, até 4 tentativas) — ver seção "Correção: dropdown de lojas vazio na tela Caixa" para o motivo.

### CNPJ e série nas ações sobre uma NF (cnpj_emp desde commit 0053; série desde 2026-08-26)
`POST /tracks/caixa/nf/<documento>/pessoa` (confirmar comprador) recebe `cnpj_emp` e `serie` no
body, enviados pelo front a partir da própria NF sendo exibida (`nf.cnpj_emp`/`nf.serie`, já
carregados na listagem — `tracks_caixa()` seleciona `serie` no `SELECT`/`GROUP BY` das notas desde
2026-08-26). Quando presentes, filtram a NF por `documento + cnpj_emp + serie` — essencial porque
`documento` se repete entre lojas **e** entre séries da mesma loja. Sem esses campos no body, cai no
comportamento antigo (primeira NF encontrada por `documento`, `LIMIT 1`, arbitrária em caso de
ambiguidade).

`GET /tracks/caixa/nf/<documento>` (itens da NF) recebe `cnpj_emp` e `serie` como query params, mesmo
propósito. O `LEFT JOIN microvix_produtos` também passou a filtrar por `portal` (a PK da tabela é
`(portal, cod_produto)`) — antes duplicava cada item quando o mesmo `cod_produto` existia em portais
diferentes (nomes de produto diferentes por portal para o mesmo código).

`DELETE .../pessoa` (remover comprador) recebe `cnpj_emp` **e `serie`** (desde 2026-09-07) como
query params — `cnpj_emp` resolve o `store_id` certo direto do `CNPJ_STORE_MAP` (antes fazia uma
busca "`documento` sozinho" em `microvix_movimento`, `LIMIT 1`, sem nem `cnpj_emp`, que podia achar
a loja errada); `serie` filtra a linha certa de `person_purchases` (`WHERE store_id AND bill AND
serie`), mesmo motivo do `POST`. Sem `serie` no request (chamada antiga/cache de front antigo), cai
no fallback por `store_id + bill` apenas.

### Tabelas envolvidas
- `faciais.person_purchases` → `(person_purchase_id, person_id, store_id, bill, serie,
  is_cancelled, is_identified)`. PK única por `(store_id, bill, serie)` desde 2026-09-07 (coluna
  `serie varchar(10)` adicionada; constraint antiga `uq_store_bill UNIQUE (store_id, bill)` trocada
  por `uq_store_bill_serie UNIQUE (store_id, bill, serie)`).

  Antes, a PK era só `(store_id, bill)` — **sem série** — e confirmar um comprador era
  inerentemente "por número de documento", não "por NF específica": se duas NFs diferentes da mesma
  loja compartilhassem o número (ver constraint acima) e ambas fossem confirmadas em momentos
  diferentes na Tela Caixa, a segunda confirmação sobrescrevia a primeira silenciosamente
  (`ON CONFLICT (store_id, bill) DO UPDATE`). Corrigido adicionando `serie` à constraint; todo
  INSERT/UPDATE/lookup em `person_purchases` no projeto (`tracks.py`, `microvix_ingest.py`) passou
  a incluir `serie` — ver `tracks_caixa_set_pessoa`, `tracks_caixa_del_pessoa`, o `confirmados` da
  listagem (`tracks_caixa`), o `bills`/join em `tracks_clientes` e `_sincronizar_person_purchases`
  (`microvix_ingest.py`).

  **Coluna `data` (existiu só algumas horas, 2026-09-07):** a correção original também adicionou
  uma coluna `data date` (`data_documento::date` da NF) e a incluiu na constraint
  (`uq_store_bill_serie_data UNIQUE (store_id, bill, serie, data)`), pra resolver os raríssimos
  casos em que `(store_id, bill, serie)` sozinho ainda seria ambíguo (mesma natureza dos "7 pares
  residuais" documentados na seção "Constraint importante" abaixo). **Removida no mesmo dia**: medição
  direta no banco mostrou **zero** grupos duplicados em `(store_id, bill, serie)` nos dados
  existentes — a constraint só com `serie` já é suficiente na prática, e o ganho de `data` não
  compensava a complexidade extra. Único efeito colateral: `faciais.vw_customer_ranking`
  (`retail_analytics`) tinha acabado de ser conectada em `pp.data` nesse meio-tempo — foi ajustada de
  volta (`CREATE OR REPLACE VIEW`, sem `pp.data`) antes da coluna ser removida, sem quebra. Ver
  `doc_faciais.sql`/`README.md` em `db-docs/lojas` pro histórico completo dessa idas-e-vindas.

  **Backfill dos dados existentes (migration única, 2026-09-07 — dados apagados no mesmo dia, ver
  seção "Reset de dados" mais abaixo; registro histórico da migration mantido aqui):** das 17.746
  linhas existentes,
  17.416 (98,1%) foram preenchidas retroativamente casando `(store_id, bill)` contra
  `microvix_movimento` (mesmos filtros da listagem: `cod_natureza_operacao='10030'`,
  `cancelado='N'`, `excluido='N'`, `tipo_transacao<>'J' OR NULL`, `codigo_cliente=1`) — só quando
  o casamento resultava em **exatamente uma** combinação `(serie, data_documento)` distinta (sem
  isso, ~80% pareciam "ambíguos" só porque a query de backfill não estava restrita a
  `codigo_cliente=1`, que é o filtro real usado pra popular a tabela). As **324 linhas
  remanescentes** (ambíguas — múltiplas combinações possíveis) e **6 sem nenhum match** ficaram com
  `serie` `NULL` — não é resolvível via query (mesma natureza dos "7 pares residuais" documentados
  na seção "Constraint importante" abaixo, só que em maior número aqui porque cobre todo o
  histórico da tabela, não só documentos vistos na Tela Caixa). Linhas com `serie` `NULL` continuam
  com o comportamento antigo (menos preciso) nas queries que dependem dela — ver comentário em
  `tracks_clientes` (`v.serie IS NULL OR v.serie = mm.serie`).

  Migration rodada ad-hoc (não commitada, via `psycopg2` direto contra o banco compartilhado) —
  não há arquivo de migration versionado neste projeto.
- `faciais.stores.cnpj` → CNPJ da loja como `int8` (sem zeros à esquerda).
- `faciais.detection_records.store_id` → loja onde a detecção ocorreu (coluna adicionada).

### Rotas da tela caixa
| Rota | Método | Descrição |
|---|---|---|
| `/tracks/caixa` ou `/m/tracks/caixa` | GET | Página principal; params: `store_id`, `data` (YYYY-MM-DD) |
| `/tracks/caixa/nf/<documento>/pessoa` ou `/m/...` | POST | Confirma comprador; body: `{person_id, cnpj_emp, serie, force?}` |
| `/tracks/caixa/nf/<documento>/pessoa` ou `/m/...` | DELETE | Remove comprador confirmado; query params: `cnpj_emp`, `serie` |
| `/tracks/caixa/pessoa/<person_id>` | GET | Dados da pessoa |
| `/tracks/caixa/nf/<documento>` | GET | Itens da NF em JSON; query params: `cnpj_emp`, `serie` |
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
   detectada depois). Exibida na chip "Chegou às" **só com a hora** (`strftime('%H:%M:%S')`, não
   `fmt_timestamp`) — a data é redundante porque a tela só lista clientes de hoje (já aparece uma
   vez só no cabeçalho, "Chegadas de hoje — DD/MM/AAAA").
2. Lista ordenada por chegada **decrescente** (mais recente primeiro).
3. Filtro de loja (`?store=<store_id>`, `0`/ausente = todas as lojas) — mesmo padrão de `tracks_lista`.
4. Dados cadastrais (nome, apelido, doc, idade, gênero, telefone, e-mail, flag, notas) sempre exibidos.
5. **Recorrência**: conta dias distintos com detecção **antes de hoje** (qualquer loja, não só a
   filtrada) via `ARRAY_AGG(DISTINCT date(created_at))` + `COUNT DISTINCT`. Cliente com 1+ visita
   anterior é "recorrente" e tem as datas dessas visitas listadas.
6. **Últimas compras + produtos/quantidades** (só para recorrentes): até 5 dias mais recentes de
   compra. Uma única consulta direto em `microvix.microvix_movimento` — **não** via
   `faciais.mv_microvix_vendas` (a materialized view usada em `vw_customer_ranking`), porque essa
   view não expõe `serie` no `SELECT`/`GROUP BY`, e é a série que efetivamente identifica a NF (ver
   abaixo). Casa cada `person_purchases` confirmado (person_id, bill, serie, cnpj) contra
   `microvix_movimento` por `(cnpj_emp, documento)` — restrito também por `serie` quando
   `person_purchases.serie` é conhecida (`v.serie IS NULL OR v.serie = mm.serie`; bills antigas sem
   backfill de série caem no comportamento antigo, menos preciso — ver "Tabelas envolvidas" na Tela
   Caixa) + `LEFT JOIN microvix.microvix_clientes_fornecedores`
   (`cf.portal = mm.portal AND cf.cod_cliente = mm.codigo_cliente`, filtro `cf.tipo_cliente IS NULL
   OR cf.tipo_cliente = 'F'`) — mesmo critério de venda válida PF que `mv_microvix_vendas` usa (ver
   seção "Definição de pessoa física" abaixo). Valor, contagem de notas e produtos/quantidades vêm
   todos do mesmo conjunto de linhas retornado, agregado em Python por `(person_id,
   data_documento::date)` — estruturalmente não tem como valor e itens virem de NFs diferentes (o bug
   original só era possível porque valor e itens vinham de duas consultas separadas). Histórico de
   compras não é limitado pela loja filtrada na tela.

   **`documento` não é uma chave confiável — nem com `cnpj_emp`.** Cada série do Microvix tem sua
   própria numeração sequencial e elas se sobrepõem constantemente — é a série (não a data) a causa
   raiz do reaproveitamento de número: `(cnpj_emp, documento)` aparece em NFs completamente
   diferentes emitidas em séries (e datas) distintas. Investigação em 2026-08-26 (motivada por um
   relato de valor/produtos incompatíveis numa compra desta tela) mediu o problema: milhares de
   pares `(cnpj_emp, documento)` ambíguos no total; restringindo a `(cnpj_emp, serie, documento)`
   cai pra **7 pares residuais** (anomalia pontual de dados de 2024 num único CNPJ/série, não o
   padrão sistêmico — não compensa tentar fechar via query, ver seção "Tela Caixa" pro mesmo número).

   Histórico da correção, por já ter sido discutido e valer como registro de decisão: a primeira
   versão do fix desambiguava só por `data_documento::date` (replicando o `GROUP BY` de
   `mv_microvix_vendas`, que perde a série) — resolvia a maioria dos casos mas deixava ~5 residuais
   de mesma data com série diferente sem resolver. Trocado depois pela abordagem direta por série
   descrita acima, que fecha esses casos por construção (não filtra por data em nenhum momento) e
   além disso simplificou o código (uma consulta em vez de duas — antes havia uma consulta separada
   via `mv_microvix_vendas` pro valor/dia e outra em `microvix_movimento` pros itens).

   **Mesma causa raiz, mesma correção aplicada na Tela Caixa** (`tracks_caixa_set_pessoa`,
   `tracks_caixa_nf_itens`, `tracks_caixa_del_pessoa`) — ver seção "Tela Caixa", "Constraint
   importante: documento não é único — nem com cnpj_emp".

7. **Ticket médio do cliente** (exibido junto do título "Últimas compras", só quando há compras):
   `valor total gasto ÷ quantidade de notas fiscais distintas` — **todo o histórico confirmado do
   cliente**, não só os 5 dias exibidos na lista (reaproveita o mesmo `linhas` já buscado pro item 6,
   sem query nova; agregado por `person_id` em vez de por `(person_id, dia)`). É a definição usual
   de ticket médio (valor/nota) — uma primeira versão dividia por quantidade de *linhas de item* em
   vez de notas fiscais, a pedido inicial do usuário, depois corrigida pra valor/nota (2026-08-27).

   **Bug corrigido em produção (2026-08-27):** `bills` só era inicializado dentro do `if chegadas:`
   — em qualquer momento em que ninguém tivesse chegado ainda no dia (`chegadas` vazio, ex.: logo
   após a virada de dia), o `if bills:` alguns blocos depois estourava `UnboundLocalError` → 500.
   Corrigido inicializando `bills = []` no mesmo nível de `clientes = []`, antes do `if chegadas:`.

### Edição de pessoa
Reaproveita **o mesmo modal e a mesma rota** de `tracks_lista.html`/`atualizar_pessoa`
(`POST /tracks/api/pessoa/<id_unico>`) — nenhum backend novo de edição foi criado para esta tela.

### Templates
`templates/tracks_clientes.html` — usado tanto por `/tracks/clientes` quanto por `/m/tracks/clientes`
(diferente de Lista/Caixa, não há template mobile separado; a página é responsiva via media query).

Foto do cliente (`.person-photo`/`.person-photo-placeholder`) é maior e mais destacada que o padrão
usado em Lista/Caixa: 132px desktop / 92px mobile (vs. 72px nas outras telas), com borda azulada e
sombra — pedido explícito (2026-08-26) para dar mais destaque visual ao rosto na tela de chegada.

**Últimas compras: cards, não tabela (2026-08-26).** A lista de "Últimas compras" (`.compra-item`)
é renderizada como uma sequência de cards empilháveis, não uma `<table>` — a primeira versão usava
uma tabela de 4 colunas fixas (Data/Notas/Valor/Produtos) que, no mobile, espremia a coluna de
produtos a ponto de ficar ilegível (relatado pelo usuário testando no celular). Cada `.compra-item`
tem uma linha de cabeçalho (`.compra-linha`: data, contagem de notas, valor) e, abaixo, os produtos
como chips (`.produto-chip`, mesmo padrão visual de `.visita-data`) num container
`flex; flex-wrap: wrap` — cada chip ocupa sua própria largura de conteúdo e quebra pra próxima linha
sozinho quando não cabe, em vez de espremer dentro de uma célula de largura fixa.

Dentro do chip, **o nome do produto (`.produto-nome`) trunca com reticências quando muito comprido,
mas a quantidade (`.produto-qtd`) nunca é cortada** — fica numa `<span>` separada com
`flex-shrink: 0` fora da área de truncamento do nome (`.produto-chip` é `inline-flex`). Uma primeira
versão truncava o chip inteiro como uma string só, o que cortava a quantidade junto quando o nome do
produto era longo (ex.: `PULVERIZADOR PLASTICO 500ML TRANSPAR...` escondia o `×2` que vinha depois).

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

**Filtros** (query string, todos combináveis e preservados na paginação): `ids`, `store`, `flag`
(botão "Clientes") e `data` (`YYYY-MM-DD`, campo "Data da coleta", desde 2026-10-24). O filtro de
`data` restringe **também as ocorrências exibidas** de cada pessoa (só as coletadas naquele dia,
via `created_at >= data AND < data+1`, aproveitando o índice) — diferente de `store`, que só decide
quais pessoas aparecem. Data inválida é ignorada.

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
5. Se a pessoa original ficou sem registros, remove-a automaticamente (mesmo comportamento de
   `mover_registro`) — **exceto** se ela tiver compras (`person_purchases`), vínculos manuais de NF
   (`manual_purchase_links`) ou duplicatas apontando pra ela, que se perderiam junto
   (`_excluir_pessoa_se_vazia`, desde 2026-10-25). Se a coleta era Intruso, o evento é fechado como
   `nova_pessoa` (ver "Estatística permanente").

Retorna: `{success, person_id, nome, pessoa_excluida}`.

---

## Duplicatas de pessoa (desde 2026-10-24)

Permite ao usuário informar que dois ou mais cadastros de `faciais.people` são a mesma pessoa.
O **vínculo** em si não move nada (nenhum `detection_record`, `person_purchases` ou
`manual_purchase_links` muda) e é reversível; mover/apagar só acontece no **tratamento**, feito
explicitamente na Tela Duplicatas (ver "Tratamento das duplicatas" abaixo).

- Coluna `faciais.people.duplicate_of_person_id` (FK `fk_people_duplicate_of` → `people`,
  `ON DELETE SET NULL`; `chk_people_duplicate_of_self` impede auto-referência). Migration rodada
  ad-hoc (sem arquivo versionado), mesmo padrão das anteriores.
- **Sempre plano, sem cadeia:** cada duplicata aponta direto pro principal; o principal tem `NULL`.
  Grupo = principal + todos que apontam pra ele (suporta 3+ duplicatas). `vincular_duplicatas`
  (`tracks.py`) mantém essa invariante: se o ID alvo já é duplicata, vincula ao principal dele; se
  um ID informado pertence a outro grupo (como principal **ou como duplicata**), o grupo inteiro
  dele é absorvido (repontado). Excluir o principal **promove a duplicata de menor ID** a principal
  (desde 2026-10-25 — `_excluir_pessoa_e_coletas` no app, e o gatilho `trg_people_quality` pra
  exclusões feitas por outros sistemas), em vez de desfazer o grupo pelo FK `SET NULL`.
  **Bug corrigido (2026-10-25):** antes só era absorvido o grupo de quem era *principal*; um ID que
  já era *duplicata* de outro grupo era movido sozinho, deixando o principal antigo isolado (caso
  real: 3184 principal + 3182 duplicata; ao adicionar 3182 a partir do 3170, o 3184 ficou fora do
  grupo). O modal agora mostra a etiqueta "+ grupo do ID X" na verificação quando o ID informado já
  é duplicata de outro grupo (`buscar_pessoa` passou a devolver `duplicata_de`).
- Independente de `people.review_status` (`'duplicate'` lá é do `retail_analytics`, só
  informativo, sem apontar qual) — esse fluxo não lê nem altera `review_status`.
- **Ainda não consolidado em lugar nenhum:** Clientes, Caixa, ranking, Quadro, pipeline de
  reconhecimento etc. continuam tratando cada cadastro separadamente. Pra consolidar, resolver o
  canônico com `COALESCE(p.duplicate_of_person_id, p.person_id)`.

Rotas (`tracks.py`):
- `GET /tracks/api/pessoa/<id>/duplicatas` → `{principal, duplicatas: [...]}` do grupo de `<id>` (cada item com `id_unico, nome, apelido, flag, duplicata_de, foto`).
- `POST /tracks/api/pessoa/<id>/duplicatas` → body `{ids: [...]}`, vincula ao grupo de `<id>`.
- `DELETE /tracks/api/pessoa/<id>/duplicatas` → desvincula `<id>` (volta a ser independente).
- `POST /tracks/api/pessoa/<id>/principal` → torna `<id>` o principal do grupo (ver Tela Duplicatas abaixo).

UI: modal único em `templates/_duplicatas_modal.html` (CSS+HTML+JS próprios, prefixo `dup-`),
incluído via `{% include %}` em `tracks_lista.html`, `tracks_tabuleiro.html` e `tracks_duplicatas.html` — diferente do modal de
edição, **não** está duplicado. Botão "⧉ Duplicatas" em cada pessoa; chip rosa "Duplicata de ID X"
(link pro principal na Lista) ou "N duplicata(s)" (abre o modal). As queries de `tracks_lista` e
`tracks_tabuleiro` trazem `duplicata_de` e `n_duplicatas`. Templates mobile (`m_lista.html`,
`m_tabuleiro.html`) não têm o recurso.

**Tela Duplicatas** (`GET /tracks/duplicatas`, `tracks_duplicatas.html`, botão "Duplicatas" no
header da Tela Dados, ao lado de "Anomalias"): lista todos os grupos (principal + duplicatas), cada
membro com foto, ID (link pra Lista), nome, flag, data de cadastro, nº de coletas e última coleta.
Grupos ordenados pela coleta mais recente de qualquer membro. Filtro `?q=` (só dígitos = ID de
qualquer membro; senão, trecho de nome/apelido de qualquer membro), aplicado em Python. Por grupo:
"Ver na Lista" (`?ids=` com todos os membros) e "Gerenciar" (abre o mesmo
`_duplicatas_modal.html`, que recarrega a página ao fechar se algo mudou). Sem paginação.
Cada duplicata tem o botão "★ Tornar principal" (`confirm()` + reload), que chama
`POST /tracks/api/pessoa/<id>/principal` (`tornar_principal`): num único `UPDATE`, o novo principal
fica com `NULL` e o principal antigo e as demais duplicatas passam a apontar pra ele (mantém o grupo
plano). Se `<id>` já é o principal, não faz nada.

### Tratamento das duplicatas (desde 2026-10-25)

Na Tela Duplicatas, cada duplicata tem um seletor de ação; "✓ Aplicar tratamento" executa o grupo
inteiro (resumo em `confirm()` antes) via `POST /tracks/api/duplicatas/<principal>/tratar`
(`tratar_duplicatas`), body `{acoes: [{id, acao, destino?}]}`. Cada duplicata pode ter uma ação
diferente; duplicata sem ação escolhida fica como está. **Tudo ou nada**: valida o lote inteiro antes
(ids precisam ser duplicatas do grupo; destino precisa existir e não pode estar saindo no mesmo
lote) e roda numa transação.

| Ação (UI) | `acao` | Efeito | Evento |
|---|---|---|---|
| Manter como duplicata | `manter` | Nada muda; tag "MANTIDA" no card | `mantida` |
| Juntar ao principal / a outro ID | `juntar` (+ `destino`, default o principal) | `_juntar_pessoa`: coletas, `person_purchases` e `manual_purchase_links` vão pro destino; campos vazios do destino recebem os da duplicata (nunca sobrescreve; nomes provisórios `AnônimoXXX`/`AXXX` contam como vazios); a duplicata é apagada | `juntada` (+ destino e nº de coletas) |
| Excluir (com as coletas) | `excluir` | `_excluir_pessoa_e_coletas`: cadastro e coletas apagados de vez. **Bloqueado (409)** se a duplicata tiver compra ou vínculo manual de NF — usar Juntar | `pessoa_excluida` (+ nº de coletas) |

"Juntar a outro ID" fora do grupo só move pro ID informado — os grupos não se juntam (decisão do
usuário). O chip âmbar "N compra(s)" no card mostra o que iria junto/bloquearia a exclusão.
`DELETE /tracks/api/pessoa/<id>` (Lista) também passou a usar `_excluir_pessoa_e_coletas` (fecha os
eventos e promove o principal), mas **não** bloqueia por compra — comportamento da Lista mantido.

---

## Anomalias de facial: Intruso / Indesejada (desde 2026-10-24)

O usuário aponta, em cada ocorrência (`detection_record`) da Tela Lista, duas anomalias
**independentes** (a mesma foto pode ter as duas):

| Anomalia | Coluna | Significado |
|---|---|---|
| Intruso | `detection_records.is_intruder` | Rosto vinculado a uma pessoa (`person_id`) que não é a dele |
| Indesejada | `detection_records.is_unwanted` | Foto ruim pra reconhecimento: de costas, obstáculo na frente, muito escura/clara, sem resolução, chapéu grande etc. |

- Ambas `boolean NOT NULL DEFAULT false`. Nada no pipeline de reconhecimento, na escolha de foto
  base ("Mudar Base") nem na Limpeza (`limpar_registros_nao_clientes`) lê essas colunas. Marcar cria
  um evento em `quality_events`; desmarcar antes de tratar apaga o evento (foi engano) — ver
  "Estatística permanente".
- `POST /tracks/api/registro/<reg_id>/anomalia` → body `{intruso?: bool, indesejada?: bool}`, só os
  campos enviados mudam; retorna o estado final `{success, intruso, indesejada}`.
- **Intruso é limpo ao mover o registro** (`mover_registro` e `nova_pessoa_de_registro` gravam
  `is_intruder = FALSE` junto com o novo `person_id`): depois de corrigido o vínculo, o registro
  deixa de ser intruso. Indesejada não muda ao mover (é propriedade da foto).
- UI (`tracks_lista.html`): segunda linha de botões no hover da ocorrência, `INTR` (laranja) e
  `INDES` (roxo), que alternam o estado. Marcado: borda laranja (intruso) / contorno roxo
  (indesejada) na foto + badge `INTRUSO`/`INDESEJADA` sempre visível. Só na Lista desktop (é a única
  tela que mostra ocorrências individuais; `m_lista.html` não tem).
- **Tela Anomalias** (`GET /tracks/anomalias`, `tracks_anomalias.html`, botão "Anomalias" no header
  da Tela Dados): lista as ocorrências marcadas coletadas num dia (`?data=YYYY-MM-DD`, default
  `hoje_local()`, data inválida cai no default) com filtro `?tipo=intruso|indesejada` (vazio =
  qualquer uma das duas). Cada caso mostra a foto coletada lado a lado com a foto base da pessoa
  vinculada (pra conferir Intruso), hora, câmera/loja, scores, link "ID X" pra Lista já filtrada
  por pessoa+data, e os mesmos botões de alternar as anomalias (mesma rota `/anomalia`) — desmarcar
  não remove o card da tela até recarregar. Sem paginação. Chip verde "histórico do dia: N
  marcada(s) · M tratada(s)" vem de `quality_events` (inclui o que já foi tratado e sumiu da lista).
- **Tratamento (desde 2026-10-25)**, na Tela Anomalias — cada card ganha uma linha de botões; card
  tratado fica esmaecido com a mensagem do que foi feito (some ao recarregar):
  - **Indesejada → "🗑 Excluir coleta"** (`DELETE /tracks/api/registro/<id>`), ou o botão em massa
    "Excluir as N indesejada(s) exibida(s)" (`POST /tracks/api/registros/excluir`, body `{ids}` —
    respeita o filtro de data/tipo da tela). Evento → `coleta_excluida`.
  - **Intruso → "↗ Vincular"** (modal com busca por ID + foto, chama a rota `/mover` já existente;
    evento → `vinculada` com destino), **"+ Nova pessoa"** (rota `/nova-pessoa` existente; evento →
    `nova_pessoa`) ou **"🗑 Excluir coleta"**. Os dois primeiros só aparecem quando a coleta está
    marcada como Intruso.
  - Coleta com as duas anomalias: excluir fecha os dois eventos.
  - `mover_registro`/`nova_pessoa_de_registro` não apagam mais a pessoa de origem vazia se ela tiver
    compras, vínculos manuais ou duplicatas (`_excluir_pessoa_se_vazia`).

---

## Relatórios (`/tracks/relatorios`, desde 2026-10-24)

Relatório estatístico de coletas, duplicatas e anomalias num período escolhido pelo usuário
(`?data_ini=&data_fim=`, default últimos 7 dias até `hoje_local()`; datas invertidas são trocadas,
inválidas caem no default; atalhos Hoje / 7 dias / 30 dias / Este mês e botão Imprimir) e,
opcionalmente, numa loja (`?store=<store_id>`) e/ou câmera (`?camera=<camera_id>`), ambos desde
2026-10-25; `0`/ausente/inválido = todas. O seletor de câmera lista só as câmeras da loja escolhida;
trocar a loja zera a câmera, e uma câmera de outra loja na URL é ignorada.

**Filtro de loja:** loja = a da coleta (`store_id`, senão a da câmera) — mesmo critério em todas as
fontes, então a soma das lojas bate com o total "Todas as lojas" (conferido na implantação). Coletas ←
`daily_collection_stats.store_id`; anomalias, duplicatas e tratamento ← `quality_events` (loja do
evento, senão da câmera; duplicata = loja da 1a coleta do cadastro duplicado); pessoas coletadas ←
`collection_person_log.store_id` (pessoa coletada nas duas lojas conta em cada uma); grupos de
duplicatas atuais ← só coletas da loja. **Novos cadastros** com loja não vêm de `daily_people_stats`
(sem loja): contam as pessoas do `collection_person_log` cuja **1a coleta** foi na loja, no dia de
criação do cadastro (ou da 1a coleta, se o cadastro já foi excluído). O "hoje N em M grupo(s)" do KPI
Duplicatas é sempre o total geral.

**Filtro de câmera:** mesmas fontes e mesmo critério do filtro de loja, pela câmera da coleta
(`daily_collection_stats.camera_id`, `quality_events.camera_id`, `collection_person_log.camera_id`,
`detection_records.camera_id`); novos cadastros = pessoas cuja 1a coleta foi na câmera. A soma das
câmeras bate com o total, exceto pessoas coletadas (a mesma pessoa passa por várias câmeras).
Botão "Relatórios" no header da Tela Dados. Função `tracks_relatorios` (`tracks.py`), template
`tracks_relatorios.html`.

**Fontes (desde 2026-10-25):** os números que precisam sobreviver a exclusões vêm das tabelas de
estatística permanente (ver seção seguinte), não mais de `detection_records`/`people`:
- Total de coletas por dia/loja/câmera ← `daily_collection_stats`; novos cadastros por dia ←
  `daily_people_stats`. Incluem o que já foi excluído (tratamento, Limpeza).
- Intruso/Indesejada ← `quality_events` pelo **dia da coleta** (`occurred_on`); "também intruso" =
  coletas com evento dos dois tipos.
- Duplicatas ← `quality_events` pelo **dia de criação do cadastro duplicado**, comparadas com os
  novos cadastros do mesmo período (`% dos novos cadastros`).
- Pessoas coletadas (KPI, coluna Pessoas por dia, coletas/pessoas por tipo de pessoa) ←
  `collection_person_log` (desde 2026-10-25, depois que excluir uma duplicata baixou o KPI): inclui
  coletas já excluídas. "Reais" resolve cada cadastro pro principal atual, ou, se o cadastro já foi
  juntado/excluído, pro `target_person_id`/`principal_person_id` do seu evento de duplicata. Coleta de
  Intruso excluída (`coleta_excluida`/`coleta_removida`) não conta — não se sabe de quem era. Tipo de
  pessoa = tipo **atual** do cadastro (ou da pessoa real, se o cadastro não existe mais).
- **Continua sobre os dados vivos:** só a tabela de grupos de duplicatas atuais com coletas no
  período. No recorte por tipo de pessoa as anomalias usam o tipo **da época da marcação**
  (`quality_events.person_type_id`), por isso essa tabela não mostra %.

Seções: KPIs, coletas por dia (gráfico + tabela com novos cadastros e duplicatas por dia),
**Tratamento** (por tipo: casos, pendentes, cada tratamento que apareceu no período e % tratado),
por loja / tipo de pessoa / câmera (taxa = anomalias ÷ total de coletas do recorte) e duplicatas
atuais. Loja: `store_id` da coleta, senão a loja da câmera (`CAMERA_STORE_MAP`); 0 = sem loja.

---

## Estatística permanente de anomalias e duplicatas (desde 2026-10-25)

Requisito do usuário: depois de qualquer tratamento (excluir, vincular, juntar…) o app precisa
continuar sabendo que a anomalia/duplicata aconteceu, em que dia e com qual coleta/pessoa, e sempre
comparar com o volume total. Como o tratamento apaga as linhas de `detection_records`/`people` (e a
Limpeza já apagava coletas, encolhendo o total por dia), o histórico, o volume total e o registro de
qual pessoa foi coletada em cada dia ficam em tabelas separadas que nunca são apagadas
(`quality_events`, `daily_collection_stats`/`daily_people_stats`, `collection_person_log`). Schema completo em `doc_faciais.sql` (`db-docs/lojas`).
Migration rodada ad-hoc em 2026-10-25 (sem arquivo versionado, mesmo padrão das anteriores).

**`faciais.quality_events`** — uma linha por ocorrência: `event_type` (`intruso`/`indesejada` por
coleta, `duplicata` por cadastro duplicado), `detection_record_id`, `person_id`, `person_type_id`
(da época), `principal_person_id`, câmera/loja, `occurred_on`/`occurred_at` (dia da coleta, ou dia de
criação do cadastro duplicado), `marked_at`, `treatment`, `treated_at`, `target_person_id`,
`affected_records`, `image_path`. **Sem FK de propósito** — guarda IDs soltos pra sobreviver às
exclusões.
- Criado ao **marcar** (`marcar_anomalia`; `vincular_duplicatas` → `_abrir_eventos_grupo` →
  função `fn_quality_dup_open`), fechado no **tratamento** com o rótulo certo **antes** da exclusão
  (`_fechar_eventos_coleta`, `_fechar_evento_duplicata`).
- `treatment`: `pendente`, `coleta_excluida`, `vinculada`, `nova_pessoa`, `juntada`,
  `pessoa_excluida`, `mantida`, e `coleta_removida`/`pessoa_removida` = excluída **fora** do
  tratamento (Limpeza, outro sistema), marcados pelos gatilhos `trg_detection_records_quality` /
  `trg_people_quality` (BEFORE DELETE) no que ainda estiver aberto.
- Desmarcar (anomalia) ou desvincular (duplicata) **enquanto pendente** apaga o evento (foi engano
  na marcação). Depois de tratado, o evento fica.
- No máximo um evento aberto por coleta/tipo (índice parcial `uq_quality_events_coleta_aberta`) e
  por cadastro duplicado (`uq_quality_events_dup_aberta`, pendente/mantida).
- "Tornar principal" (`_trocar_principal` → `fn_quality_dup_swap`): o evento do novo principal passa
  a descrever o principal antigo (mesmo `marked_at`/tratamento) — o grupo continua com N-1 eventos.
- Os 22 Indesejada, 1 Intruso e 2 duplicatas que existiam foram carregados como `pendente` na
  migration (`marked_at` = hora da migration, a marcação original não tinha data).

**`faciais.daily_collection_stats`** (`stat_date, store_id, camera_id, records`) e
**`faciais.daily_people_stats`** (`stat_date, people_created`) — volume total, incrementado pelos
gatilhos `trg_detection_records_count` / `trg_people_count` (AFTER INSERT) e nunca decrementado.
Gatilho no banco (não no app) pra contar qualquer INSERT, inclusive de outros sistemas. Granularidade
dia × loja × câmera (`0` = desconhecido). Dias anteriores a 2026-10-25 foram preenchidos com o que
ainda existia (11.982 coletas, 2.951 cadastros) — **contados a menos** onde a Limpeza já tinha
apagado coletas (aceito pelo usuário); daqui pra frente são exatos.

**`faciais.collection_person_log`** (`detection_record_id` único, `collected_on`, `person_id`,
`store_id` — loja da coleta, senão da câmera, 0 = desconhecida; `camera_id` — 0 = sem câmera; as duas
colunas vieram com os filtros de loja/câmera) — uma
linha por coleta com a pessoa vinculada, gravada/atualizada pelo gatilho
`trg_detection_records_person` (AFTER INSERT OR UPDATE OF person_id, quando `person_id` não é nulo:
cobre o pipeline, que insere sem pessoa e depois faz `UPDATE ... WHERE track_id`, e também
mover/juntar) e nunca apagada — base da contagem de pessoas coletadas do Relatório. Sem FK. Backfill
(2026-10-25) com as 11.982 coletas existentes + 1 linha sintética (`detection_record_id` NULL, no
`occurred_on`) por duplicata já excluída com coletas (só a 3170, excluída antes do log existir).

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

## Tela Dados — botão Limpeza (desde 2026-10-23)

Botão "Limpeza" em `templates/tracks_dados.html` (com `confirm()` antes de rodar) dispara
`POST /camera300/api/limpeza` e acompanha via polling de `/camera300/api/limpeza/status`. Roda em
thread background em `app.py`, mesmo padrão de `adequar_bases` (flag `_limpeza_rodando` +
`_limpeza_lock`; resultado registrado no `tracer` como `SISTEMA`).

`limpar_registros_nao_clientes()` (`db.py`) — uma única query (CTE + `DELETE ... RETURNING`):
- Escopo: pessoas com `person_type_id IS DISTINCT FROM 'C'` (inclui nulo). Clientes `'C'` nunca são tocados.
- Mantém por pessoa os **2 `detection_records` mais antigos e os 8 mais recentes** (`created_at`, desempate por `detection_record_id`); o resto é **excluído permanentemente** (sem backup).
- Se nenhum desses 10 for do `reference_track_id` da pessoa, mantém também 1 registro desse track (o da foto: com `image_path`, maior `detection_score`).
- Retorna `(registros_excluidos, pessoas_afetadas)`.
- Anomalias pendentes das coletas apagadas viram `coleta_removida` em `quality_events` (gatilho
  `trg_detection_records_quality`); o total de coletas do dia não diminui (`daily_collection_stats`),
  nem as pessoas coletadas do Relatório (`collection_person_log`).

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
- `GET /tracks/relatorios` → relatório estatístico de coletas/duplicatas/anomalias (params: `data_ini`, `data_fim`, `store`, `camera`; ver seção "Relatórios")
- `GET /tracks/anomalias` → ocorrências Intruso/Indesejada de um dia (params: `data`, `tipo`; ver seção "Anomalias de facial")
- `GET /tracks/duplicatas` → grupos de duplicatas (param: `q`; ver seção "Duplicatas de pessoa")
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
- `POST /tracks/api/registro/<reg_id>/anomalia` → marca/desmarca Intruso/Indesejada (ver seção "Anomalias de facial")
- `POST /tracks/api/registros/excluir` → exclui várias coletas (body `{ids}`; Tela Anomalias, indesejadas em massa)
- `POST /tracks/api/duplicatas/<principal>/tratar` → tratamento das duplicatas do grupo (ver "Tratamento das duplicatas")
- `GET|POST|DELETE /tracks/api/pessoa/<id_unico>/duplicatas` → grupo de duplicatas / vincular / desvincular (ver seção "Duplicatas de pessoa")
- `POST /tracks/api/pessoa/<id_unico>/principal` → torna a pessoa o principal do seu grupo de duplicatas

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
- `POST /api/limpeza` → `limpar_registros_nao_clientes()` assíncrono (409 se já rodando) — ver seção "Tela Dados — botão Limpeza"
- `GET /api/limpeza/status` → `{rodando, resultado}` (`resultado` = `{excluidos, pessoas}` ou `{erro}`)
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
- **Risco cross-projeto não verificado (2026-09-07):** a constraint `uq_store_bill UNIQUE (store_id, bill)` de `faciais.person_purchases` foi trocada por `uq_store_bill_serie UNIQUE (store_id, bill, serie)` (ver "Tabelas envolvidas" na Tela Caixa; a tabela passou brevemente por uma versão intermediária com `serie`+`data`, mas `data` foi removida no mesmo dia). O fluxo `manual_purchase_links` do `retail_analytics` "grava/corrige `faciais.person_purchases` diretamente" ao confirmar um vínculo manual — se esse código fizer `INSERT ... ON CONFLICT (store_id, bill)` explícito (não verificado nesta sessão, sem acesso ao repo do `retail_analytics`), essa gravação passa a falhar (`there is no unique or exclusion constraint matching the ON CONFLICT specification`) até ser ajustado lá também.

---

## Correção: queda de conexão SSL no microvix_ingest (2026-06-25)

**Sintoma:** `psycopg2.OperationalError: SSL connection has been closed unexpectedly` seguido de `psycopg2.InterfaceError: connection already closed` no rollback — abortava toda a sincronização.

**Causa:** `_run_portais` mantinha uma única `pg_conn` aberta por toda a sync (podendo durar 15+ min). O servidor PostgreSQL derruba sessões SSL ociosas. Quando um método falhava e o código tentava `pg_conn.rollback()` na conexão morta, gerava segunda exceção em cascata.

**Fix em `_run_portais` (`microvix_ingest.py`):** o `rollback()` agora está dentro de try/except; se falhar com `OperationalError` ou `InterfaceError`, descarta a conexão morta, pega uma nova do pool e continua os métodos restantes.

**Fix em `_sincronizar_person_purchases`:** o `faciais_conn.rollback()` e o `cur.close()` do bloco finally também foram protegidos com try/except, evitando exceção secundária quando essas conexões também caem.

---

## Correção: dropdown de lojas vazio na tela Caixa (2026-09-01)

**Sintoma:** `/tracks/caixa` carregava normalmente, mas o `<select>` de loja vinha sem nenhuma
`<option>` — impossível escolher loja e ver as NFs do dia.

**Causa:** `CNPJ_STORE_MAP` (`tracks.py`) é carregado **uma única vez**, na inicialização do
módulo (import-time), com uma query síncrona a `faciais.stores`. Em 2026-09-01 o VPS inteiro
reiniciou (reboot, não só o `camera300` — todos os serviços em `/home/workuser/` subiram no mesmo
minuto) e a disputa transitória de conexões simultâneas ao Postgres remoto (`72.60.58.241:5432`)
fez essa query falhar bem no boot do worker. Sem retry, o processo seguiu rodando indefinidamente
com `CNPJ_STORE_MAP = {}`, e a tela Caixa ficou sem lojas até o próximo restart manual.

O `try/except` que protegia a carga original imprimia um aviso (`print(...)`, sem `flush=True`) —
mas como o stdout do worker gunicorn é bufferizado por padrão (`PYTHONUNBUFFERED` não está setado
no `camera300.service`) e o processo nunca chegou a encher o buffer nem foi encerrado, o aviso
nunca apareceu no `journalctl`, dificultando o diagnóstico (nenhum erro visível nos logs apesar da
falha real ter ocorrido).

**Diagnóstico:** comparar a resposta HTTP real de `/tracks/caixa` (via `curl` direto em
`127.0.0.1:5001`, contornando o proxy) contra um `python -c "import tracks; print(tracks.CNPJ_STORE_MAP)"`
rodado à parte no mesmo venv — o segundo populava o mapa normalmente (a query em si nunca teve
problema, só a tentativa específica feita no exato momento do boot). `systemctl show camera300
--property=ActiveEnterTimestamp` confirmou o restart simultâneo de todos os serviços do VPS.

**Fix em `tracks.py`:** carga de `CNPJ_STORE_MAP` **e** de `_carregar_cameras()` (câmeras →
`CAMERA_IDS`/`CAMERAS_COMPLETO`, de onde vêm também `CAMERA_STORE_MAP`, `STORE_NAME_MAP`,
`CAMERA_STORE_NAME_MAP`, `STORE_CAMERAS_MAP`) agora tentam até 4 vezes cada
(`_BOOT_QUERY_RETRY_DELAYS = [3, 12, 20]`, constante compartilhada pelas duas, mesmo padrão de
`_RETRY_DELAYS` já usado em `app.py` pro Heimdall) antes de desistir, e os `print()` de aviso
passaram a usar `flush=True` para garantir que apareçam no `journalctl` mesmo que o processo nunca
encha o buffer de stdout sozinho. `_carregar_cameras()` é o caso mais crítico dos dois — se falhar,
`CAMERA_IDS` fica vazio e afeta a query ao Heimdall (`query_heimdall`, usada por **todo** o pipeline
de reconhecimento facial, não só a tela Caixa), então o mesmo retry foi aplicado lá.

**Remediação imediata (antes do fix):** `systemctl restart camera300` — recarrega o mapa do zero,
já que a condição de disputa de conexões é transitória (passado o boot simultâneo, a mesma query
funciona normalmente).

---

## Correção: NF com múltiplos itens aparecendo duplicada na Tela Caixa (2026-09-07)

**Sintoma:** uma mesma NF (ex.: NF #18835) aparecia como **vários cards separados** na listagem da
Tela Caixa, cada um com parte dos itens da nota — e, ao confirmar um comprador em um dos cards, o
mesmo comprador aparecia "confirmado" em todos os outros cards da mesma NF, dando impressão de bug
("está repetindo após alocação").

**Causa:** a query de listagem (`tracks_caixa`, `SELECT ... FROM microvix_movimento`) agrupava por
`GROUP BY documento, data_lancamento::date, hora_lancamento, cnpj_emp, serie` — incluindo
`hora_lancamento` (granularidade de minuto) na chave de agrupamento. Uma NF com itens lançados em
minutos diferentes (ex.: NF #18835, 5 itens entre 13:43 e 13:48, valor total R$ 128,00) virava 4
grupos distintos em vez de 1 — cada grupo mostrando só a soma/contagem dos itens daquele minuto
específico. O comprador "repetindo" era na verdade o comportamento *correto* da correção de `serie`
feita antes (ver seção "Tela Caixa" — "Tabelas envolvidas"): como os 4 cards eram, na real, a mesma
NF (mesmo `documento`+`serie`+`cnpj_emp`), o match por `(bill, serie)` os identificava corretamente
como a mesma nota — só que a tela ainda os exibia como 4 notas diferentes.

**Fix em `tracks.py` (`tracks_caixa`, rota GET):** removido `hora_lancamento` do `GROUP BY` —
agrupa só por `documento, data_lancamento::date, cnpj_emp, serie` (a chave real da NF). `hora`/`nf_dt`
passaram a usar `MIN(hora_lancamento)` (horário do primeiro item lançado) em vez do valor cru da
linha, preservando a janela de detecção (±10 min) a partir do início da compra. `itens`/`valor`
agora somam corretamente todos os itens da NF, não só os de um minuto específico.

Mesma inconsistência existia em `tracks_caixa_set_pessoa` (POST de confirmação): a busca da NF fazia
`SELECT ... LIMIT 1` sem `ORDER BY`, pegando uma linha arbitrária entre os itens da NF (com
`hora_lancamento` de qualquer um deles). Corrigido com `ORDER BY hora_lancamento ASC LIMIT 1` —
mesmo critério (horário do primeiro item) usado agora na listagem.

---

## Reset de dados: `faciais.person_purchases` truncada (2026-09-07)

Depois das correções de `serie`/`tipo_transacao`/`tipo_cliente` acima, a tabela
`faciais.person_purchases` foi **truncada por decisão do usuário** (`TRUNCATE TABLE
faciais.person_purchases RESTART IDENTITY`) — zerou todas as linhas (confirmadas e anônimas) e
reiniciou a sequence `person_purchase_id` em 1. Sem FK apontando para essa tabela, então a operação
não teve efeito cascata em outras tabelas.

**Consequência:** as ~940 confirmações de comprador feitas até então na Tela Caixa foram perdidas
(sem backup, não recuperável) e precisam ser refeitas manualmente. As linhas "anônimas" (placeholder
de NF vista, sem comprador) voltam sozinhas conforme `_sincronizar_person_purchases`
(`microvix_ingest.py`) roda nos próximos syncs incrementais (`POST /tracks/carga/sync`) — não é
preciso fazer nada manualmente pra essa parte.

Nenhuma mudança de schema envolvida — a estrutura da tabela (colunas, constraint
`uq_store_bill_serie`) continua a mesma descrita na seção "Tela Caixa" → "Tabelas envolvidas" acima.
