"""
microvix_ingest.py
==================
Versão integrada ao Flask da ingestão incremental Linx Microvix → PostgreSQL.
Roda em background thread acionada pela rota POST /tracks/carga/sync.
"""

import logging
import threading
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from xml.sax.saxutils import escape as xml_escape

import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    get_pg_conn, release_pg_conn,
    MICROVIX_CHAVE, MICROVIX_CNPJ, MICROVIX_GRUPO,
)

log = logging.getLogger(__name__)

BASE_URL      = "https://webapi.microvix.com.br/1.0/api/integracao"
WS_USER       = "linx_export"
WS_PASS       = "linx_export"
LINX_PAGE_LIMIT = 5000


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

_SESSION = _make_session()


# ---------------------------------------------------------------------------
# Status global da sincronização
# ---------------------------------------------------------------------------

_status = {
    "running":        False,
    "started_at":     None,
    "finished_at":    None,
    "current_method": None,
    "contagens":      {},
    "error":          None,
}
_status_lock = threading.Lock()


def get_status() -> dict:
    with _status_lock:
        return dict(_status)


def _set(**kwargs):
    with _status_lock:
        _status.update(kwargs)


# ---------------------------------------------------------------------------
# Comunicação com a API Linx Microvix
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
        records.append({colunas[i]: (valores[i] or "").strip() or None
                        for i in range(len(colunas))})
    return records


def _chamar_api(metodo: str, params: dict) -> list[dict]:
    body = _linx_body(metodo, params)
    resp = _SESSION.post(BASE_URL, data=body.encode("utf-8"),
                         headers={"Content-Type": "application/xml; charset=utf-8"},
                         timeout=120)
    resp.raise_for_status()
    return _parse_response(resp.content, metodo)


def _chamar_api_paginado(metodo: str, params: dict) -> list[dict]:
    todos = []
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

def _max_ts(registros: list[dict], campo="timestamp") -> int:
    vals = [int(r[campo]) for r in registros if r.get(campo)]
    return max(vals) if vals else 0


def _to_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return str(val).strip().upper() in ("1", "TRUE", "S", "SIM", "YES")


# ---------------------------------------------------------------------------
# Controle de timestamp
# ---------------------------------------------------------------------------

def _get_last_ts(conn, metodo: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_timestamp FROM microvix_sync_control WHERE metodo = %s",
            (metodo,),
        )
        row = cur.fetchone()
        return row[0] if row else 0


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
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (tabela,),
        )
        colunas_banco = {row[0] for row in cur.fetchall()}

    todas_cols = list(registros[0].keys())
    cols = [c for c in todas_cols if c in colunas_banco]
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
# Funções de ingestão por método
# ---------------------------------------------------------------------------

def _ingerir_grupo_lojas(conn) -> int:
    metodo = "LinxGrupoLojas"
    _set(current_method=metodo)
    registros = _chamar_api(metodo, {"chave": MICROVIX_CHAVE, "grupo": MICROVIX_GRUPO or ""})
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = [{k.lower(): v for k, v in r.items()} for r in registros]
    return _upsert(conn, "microvix_grupo_lojas", registros, ["portal", "empresa"])


def _ingerir_lojas(conn) -> int:
    metodo = "LinxLojas"
    _set(current_method=metodo)
    ts = _get_last_ts(conn, metodo)
    registros = _chamar_api(metodo, {"chave": MICROVIX_CHAVE, "cnpjEmp": MICROVIX_CNPJ, "timestamp": ts})
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = [{k.lower(): v for k, v in r.items()} for r in registros]
    for r in registros:
        r["centro_distribuicao"] = _to_bool(r.get("centro_distribuicao"))
    n = _upsert(conn, "microvix_lojas", registros, ["portal", "empresa"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def _ingerir_clientes(conn) -> int:
    metodo = "LinxClientesFornec"
    _set(current_method=metodo)
    ts  = _get_last_ts(conn, metodo)
    hoje = date.today()
    params = {
        "chave":        MICROVIX_CHAVE,
        "cnpjEmp":      MICROVIX_CNPJ,
        "timestamp":    ts,
        "data_inicial": str(hoje - timedelta(days=1)),
        "data_fim":     str(hoje),
    }
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = [{k.lower(): v for k, v in r.items()} for r in registros]
    for r in registros:
        r["cliente_anonimo"] = _to_bool(r.get("cliente_anonimo"))
    n = _upsert(conn, "microvix_clientes_fornecedores", registros, ["portal", "cod_cliente"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def _ingerir_movimento(conn) -> int:
    metodo = "LinxMovimento"
    _set(current_method=metodo)
    ts   = _get_last_ts(conn, metodo)
    hoje = date.today()
    params = {
        "chave":        MICROVIX_CHAVE,
        "cnpjEmp":      MICROVIX_CNPJ,
        "timestamp":    ts,
        "data_inicial": str(hoje - timedelta(days=2)),
        "data_fim":     str(hoje),
    }
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = [{k.lower(): v for k, v in r.items()} for r in registros]
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


def _ingerir_produtos(conn) -> int:
    metodo = "LinxProdutos"
    _set(current_method=metodo)
    ts   = _get_last_ts(conn, metodo)
    hoje = date.today()
    params = {
        "chave":            MICROVIX_CHAVE,
        "cnpjEmp":          MICROVIX_CNPJ,
        "timestamp":        ts,
        "dt_update_inicio": str(hoje - timedelta(days=1)),
        "dt_update_fim":    str(hoje),
    }
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = [{k.lower(): v for k, v in r.items()} for r in registros]
    for r in registros:
        r["obrigatorio_identificacao_cliente"] = _to_bool(r.get("obrigatorio_identificacao_cliente"))
    n = _upsert(conn, "microvix_produtos", registros, ["portal", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


def _ingerir_produtos_detalhes(conn) -> int:
    metodo = "LinxProdutosDetalhes"
    _set(current_method=metodo)
    ts   = _get_last_ts(conn, metodo)
    hoje = date.today()
    params = {
        "chave":               MICROVIX_CHAVE,
        "cnpjEmp":             MICROVIX_CNPJ,
        "timestamp":           ts,
        "data_mov_ini":        str(hoje - timedelta(days=2)),
        "data_mov_fim":        str(hoje),
        "retornar_saldo_zero": 0,
    }
    registros = _chamar_api_paginado(metodo, params)
    log.info("[%s] %d registros recebidos", metodo, len(registros))
    if not registros:
        return 0
    registros = [{k.lower(): v for k, v in r.items()} for r in registros]
    n = _upsert(conn, "microvix_produtos_detalhes", registros,
                ["portal", "empresa", "cod_produto"])
    _save_ts(conn, metodo, _max_ts(registros))
    return n


# ---------------------------------------------------------------------------
# Registro em microvix_carga
# ---------------------------------------------------------------------------

def _registrar_carga(conn, contagens: dict):
    cols = ", ".join(contagens.keys())
    vals = ", ".join(["%s"] * len(contagens))
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO microvix_carga ({cols}) VALUES ({vals})",
                    list(contagens.values()))
    conn.commit()
    log.info("microvix_carga registrado: %s", contagens)


# ---------------------------------------------------------------------------
# Entrypoint público — chamado pela rota Flask em background thread
# ---------------------------------------------------------------------------

_METODOS = [
    ("microvix_grupo_lojas",              _ingerir_grupo_lojas),
    ("microvix_lojas",                    _ingerir_lojas),
    ("microvix_clientes_fornecedores",    _ingerir_clientes),
    ("microvix_movimento",                _ingerir_movimento),
    ("microvix_produtos",                 _ingerir_produtos),
    ("microvix_produtos_detalhes",        _ingerir_produtos_detalhes),
]


def run_incremental():
    """Executa todos os métodos de ingestão incremental. Deve ser chamado em thread."""
    from datetime import datetime
    _set(running=True, started_at=datetime.now().isoformat(),
         finished_at=None, current_method=None, contagens={}, error=None)

    pg_conn = None
    try:
        pg_conn = get_pg_conn()
        contagens = {}
        for coluna, fn in _METODOS:
            try:
                n = fn(pg_conn)
                contagens[coluna] = n or 0
            except Exception as exc:
                log.error("Erro em %s: %s", coluna, exc, exc_info=True)
                pg_conn.rollback()
                contagens[coluna] = 0
            with _status_lock:
                _status["contagens"] = dict(contagens)

        _registrar_carga(pg_conn, contagens)
    except Exception as exc:
        log.error("Erro geral na sincronização: %s", exc, exc_info=True)
        _set(error=str(exc))
        if pg_conn:
            try:
                pg_conn.rollback()
            except Exception:
                pass
    finally:
        if pg_conn:
            release_pg_conn(pg_conn)
        _set(running=False, finished_at=datetime.now().isoformat(), current_method=None)
