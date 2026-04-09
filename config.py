import os
import mysql.connector.pooling

HEIMDALL_URL       = os.environ.get("HEIMDALL_URL",        "http://187.17.228.160:6500/api/facial/recognize-by-track")
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

DB_CONFIG = {
    'user':     os.environ.get("DB_USER",     "fefa_dev"),
    'password': os.environ.get("DB_PASSWORD", "Fd7493dt"),
    'host':     os.environ.get("DB_HOST",     "72.60.58.241"),
    'database': os.environ.get("DB_NAME",     "caras"),
    'time_zone': '-03:00',
}

_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="app_pool",
    pool_size=10,
    **DB_CONFIG
)


def get_conn():
    """Retorna uma conexão do pool. Chamar .close() devolve ao pool."""
    return _pool.get_connection()

