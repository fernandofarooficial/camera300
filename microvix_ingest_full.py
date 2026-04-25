"""
microvix_ingest_full.py
=======================
Ingestão FULL (carga completa) Linx Microvix → PostgreSQL.
Standalone script, executado manualmente.

Cria automaticamente as tabelas novas se não existirem.

Uso:
  python microvix_ingest_full.py                        # todos os métodos
  python microvix_ingest_full.py --metodo vendedores    # método específico
  python microvix_ingest_full.py --listar               # lista os métodos disponíveis
"""

import argparse
import logging
import os
import sys
import xml.etree.ElementTree as ET
from datetime import date
from xml.sax.saxutils import escape as xml_escape

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

BASE_URL = "https://webapi.microvix.com.br/1.0/api/integracao"
WS_USER  = "linx_export"
WS_PASS  = "linx_export"

CHAVE = os.environ.get("MICROVIX_CHAVE", "8F4DD752-CEED-4296-BB3B-7F9C6D9E2ED5")
CNPJ  = os.environ.get("MICROVIX_CNPJ",  "49104467000170")
GRUPO = os.environ.get("MICROVIX_GRUPO",  "ECOVILLE")
PG_DSN = os.environ.get(
    "PG_DSN",
    "postgresql://fefa_dev:Fd7493dt@72.60.58.241:5432/lojas"
)

LINX_PAGE_LIMIT = 5000
DT_INICIO_FULL  = "2026-01-01"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

SESSION = _make_session()


# ---------------------------------------------------------------------------
# Comunicação com a API
# ---------------------------------------------------------------------------

def _linx_body(metodo: str, params: dict) -> str:
    params_xml = "".join(
        f'<Parameter id="{k}">{("NULL" if v is None else xml_escape(str(v)))}</Parameter>'
        for k, v in params.items()
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        "<LinxMicrovix>"
        f'<Authentication user="{WS_USER}" password="{WS_PASS}" />'
        "<ResponseFormat>xml</ResponseFormat>"
        "<Command>"
        f"<Name>{metodo}</Name>"
        f"<Parameters>{params_xml}</Parameters>"
        "</Command>"
        "</LinxMicrovix>"
    )


def _parse_response(raw: bytes, metodo: str) -> list[dict]:
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise ValueError(f"Resposta inválida ({metodo}): {e} | Body: {raw[:300]}")

    success_el = root.find(".//ResponseSuccess")
    if success_el is not None and (success_el.text or "").strip() == "False":
        msgs = [el.text for el in root.findall(".//Message") if el.text]
        raise RuntimeError(f"API erro em {metodo}: {' | '.join(msgs)}")

    data_el = root.find(".//ResponseData")
    if data_el is None:
        return []
    col_el = data_el.find("C")
    if col_el is None:
        return []
    colunas = [d.text or "" for d in col_el.findall("D")]

    records = []
    for row_el in data_el.findall("R"):
        valores = [d.text for d in row_el.findall("D")]
        while len(valores) < len(colunas):
            valores.append(None)
        records.append({
            colunas[i]: (valores[i] or "").strip() or None
            for i in range(len(colunas))
        })
    return records


def _chamar_api(metodo: str, params: dict) -> list[dict]:
    body = _linx_body(metodo, params)
    resp = SESSION.post(
        BASE_URL,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/xml; charset=utf-8"},
        timeout=120,
    )
    resp.raise_for_status()
    return _parse_response(resp.content, metodo)


def _chamar_api_paginado(metodo: str, params: dict) -> list[dict]:
    todos: list[dict] = []
    params = dict(params)
    pagina = 1
    while True:
        registros = _chamar_api(metodo, params)
        todos.extend(registros)
        if len(registros) < LINX_PAGE_LIMIT:
            break
        max_ts = _max_ts(registros)
        if max_ts == 0:
            break
        log.info("[%s] pág %d: %d acumulados, próx ts=%s", metodo, pagina, len(todos), max_ts)
        params["timestamp"] = max_ts
        pagina += 1
    return todos


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _max_ts(registros: list[dict], campo: str = "timestamp") -> int:
    vals = [int(r[campo]) for r in registros if r.get(campo)]
    return max(vals) if vals else 0


def _to_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return str(val).strip().upper() in ("1", "TRUE", "S", "SIM", "YES")


def _lower_keys(registros: list[dict]) -> list[dict]:
    return [{k.lower(): v for k, v in r.items()} for r in registros]


# ---------------------------------------------------------------------------
# Controle de timestamp (atualiza para que o incremental parta daqui)
# ---------------------------------------------------------------------------

def _save_ts(conn, metodo: str, ts: int):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO microvix_sync_control (metodo, last_timestamp, last_sync_at)
               VALUES (%s, %s, NOW())
               ON CONFLICT (metodo) DO UPDATE
                 SET last_timestamp = EXCLUDED.last_timestamp,
                     last_sync_at   = NOW()""",
            (metodo, ts),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Upsert genérico
# ---------------------------------------------------------------------------

def _upsert(conn, tabela: str, registros: list[dict], pk_cols: list[str]) -> int:
    if not registros:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'microvix' AND table_name = %s",
            (tabela,),
        )
        colunas_banco = {row[0] for row in cur.fetchall()}

    todas_cols = list(registros[0].keys())
    cols = [c for c in todas_cols if c in colunas_banco]
    ignoradas = set(todas_cols) - set(cols)
    if ignoradas:
        log.debug("[%s] colunas ignoradas: %s", tabela, ignoradas)

    col_list = ", ".join(cols)
    val_tmpl = ", ".join(["%s"] * len(cols))
    update   = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_cols)
    sql = (
        f"INSERT INTO {tabela} ({col_list}) VALUES ({val_tmpl}) "
        f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {update}"
    )
    rows = [[r.get(c) for c in cols] for r in registros]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# DDL — tabelas novas (criadas apenas se não existirem)
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS microvix_clientes_fornec_campos_adicionais (
        portal      INT         NOT NULL,
        cod_cliente INT         NOT NULL,
        campo       VARCHAR(50) NOT NULL,
        valor       VARCHAR(100),
        PRIMARY KEY (portal, cod_cliente, campo)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_clientes_fornec_classes (
        portal      INT      NOT NULL,
        cod_cliente INT      NOT NULL,
        cod_classe  SMALLINT NOT NULL,
        nome_classe VARCHAR(50),
        timestamp   BIGINT,
        PRIMARY KEY (portal, cod_cliente, cod_classe)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_fidelidade (
        portal                     INT         NOT NULL,
        cnpj_emp                   VARCHAR(14),
        id_fidelidade_parceiro_log INT         NOT NULL,
        data_transacao             TIMESTAMP,
        operacao                   SMALLINT,
        aprovado_barramento        VARCHAR(4),
        valor_monetario            NUMERIC(15,4),
        numero_cartao              VARCHAR(100),
        identificador_movimento    UUID,
        timestamp                  BIGINT,
        PRIMARY KEY (portal, id_fidelidade_parceiro_log)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_vendedores (
        portal               INT    NOT NULL,
        cod_vendedor         INT    NOT NULL,
        nome_vendedor        VARCHAR(50),
        tipo_vendedor        CHAR(1),
        end_vend_rua         VARCHAR(250),
        end_vend_numero      INT,
        end_vend_complemento VARCHAR(60),
        end_vend_bairro      VARCHAR(60),
        end_vend_cep         CHAR(9),
        end_vend_cidade      VARCHAR(60),
        end_vend_uf          CHAR(2),
        fone_vendedor        VARCHAR(30),
        mail_vendedor        VARCHAR(50),
        dt_upd               TIMESTAMP,
        cpf_vendedor         VARCHAR(11),
        ativo                CHAR(1),
        data_admissao        TIMESTAMP,
        data_saida           TIMESTAMP,
        timestamp            BIGINT,
        matricula            VARCHAR(30),
        id_tipo_venda        SMALLINT,
        descricao_tipo_venda VARCHAR(100),
        cargo                VARCHAR(20),
        PRIMARY KEY (portal, cod_vendedor)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_metas_vendedores (
        portal              INT         NOT NULL,
        cnpj_emp            VARCHAR(14) NOT NULL,
        id_meta             INT         NOT NULL,
        descricao_meta      VARCHAR(50),
        data_inicial_meta   TIMESTAMP,
        data_final_meta     TIMESTAMP,
        valor_meta_loja     NUMERIC(15,4),
        valor_meta_vendedor NUMERIC(15,4),
        cod_vendedor        INT,
        timestamp           BIGINT,
        PRIMARY KEY (portal, cnpj_emp, id_meta)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_produtos_depositos (
        portal                   INT         NOT NULL,
        cod_deposito             INT         NOT NULL,
        nome_deposito            VARCHAR(50),
        disponivel               BOOLEAN,
        disponivel_transferencia BOOLEAN,
        timestamp                BIGINT,
        PRIMARY KEY (portal, cod_deposito)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_produtos_inventario (
        portal          INT         NOT NULL,
        cnpj_emp        VARCHAR(14) NOT NULL,
        cod_produto     BIGINT      NOT NULL,
        cod_barra       VARCHAR(20),
        quantidade      FLOAT,
        cod_deposito    INT,
        empresa         INT,
        data_inventario DATE,
        PRIMARY KEY (portal, cnpj_emp, cod_produto)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_produtos_promocoes (
        portal                 INT         NOT NULL,
        cnpj_emp               VARCHAR(14) NOT NULL,
        cod_produto            BIGINT      NOT NULL,
        id_campanha            BIGINT      NOT NULL,
        preco_promocao         NUMERIC(15,4),
        data_inicio_promocao   TIMESTAMP,
        data_termino_promocao  TIMESTAMP,
        data_cadastro_promocao TIMESTAMP,
        promocao_ativa         CHAR(1),
        nome_campanha          VARCHAR(60),
        promocao_opcional      BOOLEAN,
        custo_total_campanha   NUMERIC(15,4),
        PRIMARY KEY (portal, cnpj_emp, cod_produto, id_campanha)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_produtos_tabelas (
        portal               INT         NOT NULL,
        cnpj_emp             VARCHAR(14) NOT NULL,
        id_tabela            INT         NOT NULL,
        nome_tabela          VARCHAR(50),
        ativa                CHAR(1),
        timestamp            BIGINT,
        tipo_tabela          CHAR(1),
        codigo_integracao_ws VARCHAR(50),
        PRIMARY KEY (portal, cnpj_emp, id_tabela)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_produtos_tabelas_precos (
        portal      INT         NOT NULL,
        cnpj_emp    VARCHAR(14) NOT NULL,
        id_tabela   INT         NOT NULL,
        cod_produto BIGINT      NOT NULL,
        precovenda  NUMERIC(15,4),
        timestamp   BIGINT,
        PRIMARY KEY (portal, cnpj_emp, id_tabela, cod_produto)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_faturas (
        portal                        INT         NOT NULL,
        cnpj_emp                      VARCHAR(14) NOT NULL,
        codigo_fatura                 BIGINT      NOT NULL,
        data_emissao                  TIMESTAMP,
        cod_cliente                   INT,
        nome_cliente                  VARCHAR(60),
        data_vencimento               TIMESTAMP,
        data_baixa                    TIMESTAMP,
        valor_fatura                  NUMERIC(15,4),
        valor_pago                    NUMERIC(15,4),
        valor_desconto                NUMERIC(15,4),
        valor_juros                   NUMERIC(15,4),
        documento                     INT,
        serie                         VARCHAR(10),
        ecf                           INT,
        identificador                 UUID,
        nsu                           VARCHAR(50),
        cod_autorizacao               VARCHAR(50),
        documento_sem_tef             VARCHAR(350),
        autorizacao_sem_tef           VARCHAR(30),
        observacao                    TEXT,
        qtde_parcelas                 INT,
        ordem_parcela                 INT,
        receber_pagar                 CHAR(1),
        vendedor                      INT,
        excluido                      CHAR(1),
        cancelado                     CHAR(1),
        plano                         INT,
        conta_credito                 BIGINT,
        conta_debito                  BIGINT,
        conta_fluxo                   BIGINT,
        cod_historico                 BIGINT,
        forma_pgto                    VARCHAR(50),
        ordem_cartao                  SMALLINT,
        banco_codigo                  VARCHAR(10),
        banco_agencia                 VARCHAR(30),
        banco_conta                   VARCHAR(30),
        banco_autorizacao_garantidora VARCHAR(30),
        numero_bilhete_seguro         VARCHAR(30),
        timestamp                     BIGINT,
        empresa                       INT,
        id_categorias_financeiras     INT,
        taxa_financeira               NUMERIC(15,4),
        valor_abatimento              FLOAT,
        valor_multa                   NUMERIC(15,4),
        centrocusto                   INT,
        perc_taxa_adquirente          FLOAT,
        fatura_origem_importacao_erp  VARCHAR(50),
        PRIMARY KEY (portal, cnpj_emp, codigo_fatura)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_pedidos_venda (
        portal                  INT         NOT NULL,
        cnpj_emp                VARCHAR(14) NOT NULL,
        transacao               INT         NOT NULL,
        cod_produto             BIGINT      NOT NULL,
        cod_pedido              INT,
        data_lancamento         TIMESTAMP,
        hora_lancamento         CHAR(5),
        usuario                 INT,
        codigo_cliente          INT,
        quantidade              FLOAT,
        valor_unitario          NUMERIC(15,4),
        cod_vendedor            INT,
        valor_frete             NUMERIC(15,4),
        valor_total             NUMERIC(15,4),
        desconto_item           NUMERIC(15,4),
        cod_plano_pagamento     INT,
        plano_pagamento         VARCHAR(35),
        obs                     TEXT,
        aprovado                CHAR(1),
        cancelado               CHAR(1),
        data_aprovacao          TIMESTAMP,
        data_alteracao          TIMESTAMP,
        tipo_frete              SMALLINT,
        natureza_operacao       VARCHAR(60),
        tabela_preco            INT,
        nome_tabela_preco       VARCHAR(50),
        previsao_entrega        TIMESTAMP,
        realizado_por           INT,
        pontuacao_ser           INT,
        venda_externa           CHAR(1),
        nf_gerada               TEXT,
        status                  CHAR(1),
        numero_projeto_officina VARCHAR(50),
        cod_natureza_operacao   CHAR(10),
        margem_contribuicao     NUMERIC(15,4),
        doc_origem              INT,
        posicao_item            INT,
        orcamento_origem        INT,
        transacao_origem        INT,
        timestamp               BIGINT,
        desconto                NUMERIC(15,4),
        transacao_ws            INT,
        empresa                 INT,
        transportador           BIGINT,
        deposito                INT,
        PRIMARY KEY (portal, cnpj_emp, transacao, cod_produto)
    )""",
    """CREATE TABLE IF NOT EXISTS microvix_pedidos_compra (
        portal                  INT         NOT NULL,
        cnpj_emp                VARCHAR(14) NOT NULL,
        cod_pedido              INT         NOT NULL,
        cod_produto             BIGINT      NOT NULL,
        data_pedido             TIMESTAMP,
        transacao               INT,
        usuario                 INT,
        codigo_fornecedor       INT,
        quantidade              FLOAT,
        valor_unitario          NUMERIC(15,4),
        cod_comprador           INT,
        valor_frete             NUMERIC(15,4),
        valor_total             NUMERIC(15,4),
        cod_plano_pagamento     INT,
        plano_pagamento         VARCHAR(35),
        obs                     TEXT,
        aprovado                CHAR(1),
        cancelado               CHAR(1),
        encerrado               CHAR(1),
        data_aprovacao          TIMESTAMP,
        numero_ped_fornec       VARCHAR(15),
        tipo_frete              CHAR(1),
        natureza_operacao       VARCHAR(73),
        previsao_entrega        TIMESTAMP,
        numero_projeto_officina VARCHAR(50),
        status_pedido           CHAR(1),
        qtde_entregue           FLOAT,
        descricao_frete         VARCHAR(40),
        integrado_linx          BOOLEAN,
        nf_gerada               INT,
        timestamp               BIGINT,
        empresa                 INT,
        nf_origem_ws            VARCHAR(30),
        PRIMARY KEY (portal, cnpj_emp, cod_pedido, cod_produto)
    )""",
]


def _ensure_tables(conn):
    with conn.cursor() as cur:
        for ddl in _DDL_STATEMENTS:
            cur.execute(ddl)
    conn.commit()
    log.info("Tabelas verificadas/criadas com sucesso.")


# ---------------------------------------------------------------------------
# Funções de ingestão — tabelas existentes
# ---------------------------------------------------------------------------

def ingerir_grupo_lojas(conn) -> int:
    metodo = "LinxGrupoLojas"
    log.info("[%s] consultando", metodo)
    registros = _chamar_api(metodo, {"chave": CHAVE, "grupo": GRUPO or ""})
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    return _upsert(conn, "microvix_grupo_lojas", registros, ["portal", "empresa"])


def ingerir_lojas(conn) -> int:
    metodo = "LinxLojas"
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api(metodo, {"chave": CHAVE, "cnpjEmp": CNPJ, "timestamp": 0})
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    for r in registros:
        r["centro_distribuicao"] = _to_bool(r.get("centro_distribuicao"))
    n = _upsert(conn, "microvix_lojas", registros, ["portal", "empresa"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_clientes(conn) -> int:
    metodo = "LinxClientesFornec"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "timestamp":    0,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    for r in registros:
        r["cliente_anonimo"] = _to_bool(r.get("cliente_anonimo"))
    n = _upsert(conn, "microvix_clientes_fornecedores", registros, ["portal", "cod_cliente"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_movimento(conn) -> int:
    metodo = "LinxMovimento"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "timestamp":    0,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    bool_campos = [
        "forma_dinheiro", "forma_cheque", "forma_cheque_prazo", "forma_cartao",
        "forma_crediario", "forma_convenio", "forma_pix", "forma_deposito_bancario",
        "mob_checkout", "item_promocional", "acrescimo_item",
    ]
    for r in registros:
        for c in bool_campos:
            r[c] = _to_bool(r.get(c))
    n = _upsert(conn, "microvix_movimento", registros,
                ["portal", "cnpj_emp", "transacao", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_produtos(conn) -> int:
    metodo = "LinxProdutos"
    params = {
        "chave":            CHAVE,
        "cnpjEmp":          CNPJ,
        "timestamp":        0,
        "dt_update_inicio": DT_INICIO_FULL,
        "dt_update_fim":    str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    for r in registros:
        r["obrigatorio_identificacao_cliente"] = _to_bool(
            r.get("obrigatorio_identificacao_cliente")
        )
    n = _upsert(conn, "microvix_produtos", registros, ["portal", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_produtos_detalhes(conn) -> int:
    metodo = "LinxProdutosDetalhes"
    params = {
        "chave":               CHAVE,
        "cnpjEmp":             CNPJ,
        "timestamp":           0,
        "data_mov_ini":        DT_INICIO_FULL,
        "data_mov_fim":        str(date.today()),
        "retornar_saldo_zero": 0,
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_produtos_detalhes", registros,
                ["portal", "empresa", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


# ---------------------------------------------------------------------------
# Funções de ingestão — tabelas novas
# ---------------------------------------------------------------------------

def ingerir_clientes_campos_adicionais(conn) -> int:
    metodo = "LinxClientesFornecCamposAdicionais"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    return _upsert(conn, "microvix_clientes_fornec_campos_adicionais", registros,
                   ["portal", "cod_cliente", "campo"])


def ingerir_clientes_classes(conn) -> int:
    metodo = "LinxClientesFornecClasses"
    params = {
        "chave":     CHAVE,
        "cnpjEmp":   CNPJ,
        "timestamp": 0,
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_clientes_fornec_classes", registros,
                ["portal", "cod_cliente", "cod_classe"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_fidelidade(conn) -> int:
    metodo = "LinxFidelidade"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "timestamp":    0,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_fidelidade", registros,
                ["portal", "id_fidelidade_parceiro_log"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_vendedores(conn) -> int:
    metodo = "LinxVendedores"
    params = {
        "chave":     CHAVE,
        "cnpjEmp":   CNPJ,
        "timestamp": 0,
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_vendedores", registros, ["portal", "cod_vendedor"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_metas_vendedores(conn) -> int:
    metodo = "LinxMetasVendedores"
    params = {
        "chave":            CHAVE,
        "cnpjEmp":          CNPJ,
        "timestamp":        0,
        "data_inicial_meta": DT_INICIO_FULL,
        "data_fim_meta":    str(date.today()),
    }
    log.info("[%s] consultando (full, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_metas_vendedores", registros,
                ["portal", "cnpj_emp", "id_meta"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_produtos_depositos(conn) -> int:
    metodo = "LinxProdutosDepositos"
    params = {
        "chave":     CHAVE,
        "cnpjEmp":   CNPJ,
        "timestamp": 0,
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    for r in registros:
        r["disponivel"]               = _to_bool(r.get("disponivel"))
        r["disponivel_transferencia"] = _to_bool(r.get("disponivel_transferencia"))
    n = _upsert(conn, "microvix_produtos_depositos", registros, ["portal", "cod_deposito"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_produtos_inventario(conn) -> int:
    """Snapshot do estoque na data de hoje. Não usa timestamp."""
    metodo = "LinxProdutosInventario"
    hoje = str(date.today())
    params = {
        "chave":           CHAVE,
        "cnpjEmp":         CNPJ,
        "data_inventario": hoje,
    }
    log.info("[%s] consultando inventário em %s", metodo, hoje)
    registros = _chamar_api(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    for r in registros:
        r["data_inventario"] = hoje
    return _upsert(conn, "microvix_produtos_inventario", registros,
                   ["portal", "cnpj_emp", "cod_produto"])


def ingerir_produtos_promocoes(conn) -> int:
    """Busca promoções ativas e inativas separadamente e combina os resultados."""
    metodo = "LinxProdutosPromocoes"
    hoje = str(date.today())
    todos: list[dict] = []

    for flag_ativa in ("S", "N"):
        params = {
            "chave":           CHAVE,
            "cnpjEmp":         CNPJ,
            "data_cad_inicial": DT_INICIO_FULL,
            "data_cad_fim":    hoje,
            "data_vig_inicial": DT_INICIO_FULL,
            "data_vig_fim":    hoje,
            "promocao_ativa":  flag_ativa,
        }
        log.info("[%s] consultando promocao_ativa=%s", metodo, flag_ativa)
        registros = _chamar_api(metodo, params)
        log.info("[%s] %d registros (ativa=%s)", metodo, len(registros), flag_ativa)
        todos.extend(registros)

    if not todos:
        return 0
    todos = _lower_keys(todos)
    for r in todos:
        r["promocao_opcional"] = _to_bool(r.get("promocao_opcional"))
    return _upsert(conn, "microvix_produtos_promocoes", todos,
                   ["portal", "cnpj_emp", "cod_produto", "id_campanha"])


def ingerir_produtos_tabelas(conn) -> int:
    metodo = "LinxProdutosTabelas"
    params = {
        "chave":     CHAVE,
        "cnpjEmp":   CNPJ,
        "timestamp": 0,
    }
    log.info("[%s] consultando (full, ts=0)", metodo)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_produtos_tabelas", registros,
                ["portal", "cnpj_emp", "id_tabela"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_produtos_tabelas_precos(conn) -> int:
    """id_tabela é obrigatório na API — itera sobre todas as tabelas cadastradas."""
    metodo = "LinxProdutosTabelasPrecos"

    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT id_tabela FROM microvix_produtos_tabelas")
        tabela_ids = [row[0] for row in cur.fetchall()]

    if not tabela_ids:
        log.warning("[%s] nenhuma tabela encontrada em microvix_produtos_tabelas — execute "
                    "ingerir_produtos_tabelas primeiro.", metodo)
        return 0

    log.info("[%s] iterando sobre %d tabelas de preço", metodo, len(tabela_ids))
    total = 0
    for id_tabela in tabela_ids:
        params = {
            "chave":     CHAVE,
            "cnpjEmp":   CNPJ,
            "timestamp": 0,
            "id_tabela": id_tabela,
        }
        registros = _chamar_api_paginado(metodo, params)
        if not registros:
            continue
        registros = _lower_keys(registros)
        n = _upsert(conn, "microvix_produtos_tabelas_precos", registros,
                    ["portal", "cnpj_emp", "id_tabela", "cod_produto"])
        log.info("[%s] tabela %d: %d registros", metodo, id_tabela, n)
        total += n
        if registros:
            _save_ts(conn, f"{metodo}_tabela_{id_tabela}", _max_ts(registros))

    _save_ts(conn, metodo, 0)
    return total


def ingerir_faturas(conn) -> int:
    metodo = "LinxFaturas"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "timestamp":    0,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_faturas", registros,
                ["portal", "cnpj_emp", "codigo_fatura"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_pedidos_venda(conn) -> int:
    metodo = "LinxPedidosVenda"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "timestamp":    0,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    n = _upsert(conn, "microvix_pedidos_venda", registros,
                ["portal", "cnpj_emp", "transacao", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def ingerir_pedidos_compra(conn) -> int:
    metodo = "LinxPedidosCompra"
    params = {
        "chave":        CHAVE,
        "cnpjEmp":      CNPJ,
        "timestamp":    0,
        "data_inicial": DT_INICIO_FULL,
        "data_fim":     str(date.today()),
    }
    log.info("[%s] consultando (full, ts=0, desde %s)", metodo, DT_INICIO_FULL)
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = _lower_keys(registros)
    for r in registros:
        r["integrado_linx"] = _to_bool(r.get("integrado_linx"))
    n = _upsert(conn, "microvix_pedidos_compra", registros,
                ["portal", "cnpj_emp", "cod_pedido", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


# ---------------------------------------------------------------------------
# Registro de execução
# ---------------------------------------------------------------------------

def _registrar_log(conn, contagens: dict):
    linhas = "\n".join(f"  {k}: {v}" for k, v in contagens.items())
    log.info("Carga full concluída:\n%s", linhas)

    with conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS microvix_carga_full (
                id          SERIAL PRIMARY KEY,
                contagens   JSONB,
                executado_em TIMESTAMPTZ DEFAULT NOW()
            )"""
        )
        cur.execute(
            "INSERT INTO microvix_carga_full (contagens) VALUES (%s)",
            (psycopg2.extras.Json(contagens),),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Mapa de métodos
# ---------------------------------------------------------------------------

METODOS_MAP = {
    "grupo_lojas":                 ingerir_grupo_lojas,
    "lojas":                       ingerir_lojas,
    "clientes":                    ingerir_clientes,
    "clientes_campos_adicionais":  ingerir_clientes_campos_adicionais,
    "clientes_classes":            ingerir_clientes_classes,
    "fidelidade":                  ingerir_fidelidade,
    "vendedores":                  ingerir_vendedores,
    "metas_vendedores":            ingerir_metas_vendedores,
    "movimento":                   ingerir_movimento,
    "produtos":                    ingerir_produtos,
    "produtos_detalhes":           ingerir_produtos_detalhes,
    "produtos_depositos":          ingerir_produtos_depositos,
    "produtos_inventario":         ingerir_produtos_inventario,
    "produtos_promocoes":          ingerir_produtos_promocoes,
    "produtos_tabelas":            ingerir_produtos_tabelas,
    "produtos_tabelas_precos":     ingerir_produtos_tabelas_precos,
    "faturas":                     ingerir_faturas,
    "pedidos_venda":               ingerir_pedidos_venda,
    "pedidos_compra":              ingerir_pedidos_compra,
}

# produtos_tabelas_precos depende de produtos_tabelas — mantemos a ordem
_ORDEM_EXECUCAO = [
    "grupo_lojas",
    "lojas",
    "clientes",
    "clientes_campos_adicionais",
    "clientes_classes",
    "fidelidade",
    "vendedores",
    "metas_vendedores",
    "movimento",
    "produtos",
    "produtos_detalhes",
    "produtos_depositos",
    "produtos_inventario",
    "produtos_promocoes",
    "produtos_tabelas",
    "produtos_tabelas_precos",
    "faturas",
    "pedidos_venda",
    "pedidos_compra",
]


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingestão FULL Linx Microvix → PostgreSQL"
    )
    parser.add_argument(
        "--metodo",
        choices=list(METODOS_MAP.keys()),
        help="Executa apenas o método informado (padrão: todos)",
    )
    parser.add_argument(
        "--listar",
        action="store_true",
        help="Lista os métodos disponíveis e sai",
    )
    args = parser.parse_args()

    if args.listar:
        print("Métodos disponíveis:")
        for nome in _ORDEM_EXECUCAO:
            print(f"  {nome}")
        sys.exit(0)

    conn = psycopg2.connect(
        PG_DSN,
        options="-c search_path=microvix -c timezone=America/Sao_Paulo",
    )

    try:
        _ensure_tables(conn)

        metodos = (
            {args.metodo: METODOS_MAP[args.metodo]}
            if args.metodo
            else {nome: METODOS_MAP[nome] for nome in _ORDEM_EXECUCAO}
        )

        contagens: dict = {}
        for nome, fn in metodos.items():
            log.info("=== Iniciando: %s ===", nome)
            try:
                n = fn(conn)
                contagens[nome] = n or 0
                log.info("=== Concluído: %s → %d registros ===", nome, contagens[nome])
            except Exception as exc:
                log.error("Erro em %s: %s", nome, exc, exc_info=True)
                conn.rollback()
                contagens[nome] = -1

        if contagens:
            _registrar_log(conn, contagens)

    finally:
        conn.close()

    log.info("Ingestão full concluída.")


if __name__ == "__main__":
    main()
