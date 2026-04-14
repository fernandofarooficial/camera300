import os
import mysql.connector.pooling
import psycopg2
import psycopg2.pool

HEIMDALL_URL        = os.environ.get("HEIMDALL_URL",        "http://187.17.228.160:6500/api/facial/recognize-by-track")
HEIMDALL_IMAGE_BASE = os.environ.get("HEIMDALL_IMAGE_BASE", "http://187.17.228.160:6500/api/facial/images")

# Data de início das consultas ao Heimdall. Pode ser sobrescrita via variável de ambiente.
# Formato ISO: YYYY-MM-DD
HEIMDALL_START_DATE = os.environ.get("HEIMDALL_START_DATE", "2026-04-02")
HEIMDALL_END_DATE   = os.environ.get("HEIMDALL_END_DATE",   "2026-12-31")

# Flag atribuída a novas pessoas criadas automaticamente pelo sistema.
FLAG_NOVO_ANONIMO = os.environ.get("FLAG_NOVO_ANONIMO", "C")

# Score mínimo de detecção facial para aceitar um registro.
SCORE_MINIMO = float(os.environ.get("SCORE_MINIMO", "0.73"))

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "8608684261:AAEuQYGMbSA9ytbGFCvV-jl87cPhRHw1le4")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003759902077")

ZIONS_API_URL = os.environ.get("ZIONS_API_URL", "http://187.17.228.160:6500/api")
ZIONS_TOKEN   = os.environ.get("ZIONS_TOKEN",   "")

# ── Linx Microvix ──────────────────────────────────────────────────────────────
MICROVIX_CHAVE = os.environ.get("MICROVIX_CHAVE", "8F4DD752-CEED-4296-BB3B-7F9C6D9E2ED5")
MICROVIX_CNPJ  = os.environ.get("MICROVIX_CNPJ",  "49104467000170")
MICROVIX_GRUPO = os.environ.get("MICROVIX_GRUPO", "ECOVILLE")

DB_CONFIG = {
    'user':     os.environ.get("DB_USER",     "fefa_dev"),
    'password': os.environ.get("DB_PASSWORD", "Fd7493dt"),
    'host':     os.environ.get("DB_HOST",     "72.60.58.241"),
    'database': os.environ.get("DB_NAME",     "caras"),
    'time_zone': '-03:00',
}

import threading

_pool = None
_pool_lock = threading.Lock()


def get_conn():
    """Retorna uma conexão do pool MySQL. Cria o pool na primeira chamada."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="app_pool",
                    pool_size=10,
                    **DB_CONFIG,
                )
    return _pool.get_connection()


# ── PostgreSQL (microvix) ──────────────────────────────────────────────────────
PG_DSN = os.environ.get(
    "PG_DSN",
    "postgresql://fefa_dev:Fd7493dt@72.60.58.241:5432/lojas",
)

_pg_pool = None
_pg_pool_lock = threading.Lock()


def get_pg_conn():
    """Retorna uma conexão PostgreSQL do pool. Cria o pool na primeira chamada."""
    global _pg_pool
    if _pg_pool is None:
        with _pg_pool_lock:
            if _pg_pool is None:
                _pg_pool = psycopg2.pool.ThreadedConnectionPool(
                    1, 5, PG_DSN, options="-c timezone=America/Sao_Paulo -c search_path=microvix"
                )
    return _pg_pool.getconn()


def release_pg_conn(conn):
    """Devolve a conexão PostgreSQL ao pool."""
    _pg_pool.putconn(conn)


# ── PostgreSQL (faciais) ───────────────────────────────────────────────────────
_faciais_pool = None
_faciais_pool_lock = threading.Lock()


def get_faciais_conn():
    """Retorna uma conexão PostgreSQL do pool (schema faciais)."""
    global _faciais_pool
    if _faciais_pool is None:
        with _faciais_pool_lock:
            if _faciais_pool is None:
                _faciais_pool = psycopg2.pool.ThreadedConnectionPool(
                    1, 10, PG_DSN, options="-c timezone=America/Sao_Paulo -c search_path=faciais"
                )
    return _faciais_pool.getconn()


def release_faciais_conn(conn):
    """Devolve a conexão PostgreSQL (faciais) ao pool."""
    _faciais_pool.putconn(conn)

