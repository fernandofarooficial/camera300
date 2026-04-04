import mysql.connector.pooling

HEIMDALL_URL = "http://187.17.228.160:6500/api/facial/recognize-by-track"
HEIMDALL_IMAGE_BASE = "http://187.17.228.160:6500/api/facial/images"

DB_CONFIG = {
    'user': 'fefa_dev',
    'password': 'Fd7493dt',
    'host': '72.60.58.241',
    'database': 'caras',
    'time_zone': '-03:00'
}

_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="app_pool",
    pool_size=10,
    **DB_CONFIG
)


def get_conn():
    """Retorna uma conexão do pool. Chamar .close() devolve ao pool."""
    return _pool.get_connection()

