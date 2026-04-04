"""
tracer.py — Log de execução por track_id.

Uso em qualquer módulo:
    from tracer import trace
    trace(track_id, "minha mensagem")
"""
import json
from datetime import datetime, timezone, timedelta

_TZ_SP = timezone(timedelta(hours=-3))

_MAX_ENTRIES = 300   # entradas totais mantidas em memória
_broadcast_queue = None
trace_entries = []   # lista plana de todas as entradas (ordem cronológica)


def set_queue(q):
    """Conecta o tracer à fila SSE do app. Chamar uma vez no startup."""
    global _broadcast_queue
    _broadcast_queue = q


def trace(track_id, msg):
    """Registra uma mensagem de trace para o track_id dado."""
    entry = {
        "type": "trace",
        "track_id": str(track_id),
        "time": datetime.now(_TZ_SP).strftime('%H:%M:%S'),
        "msg": str(msg),
    }
    trace_entries.append(entry)
    if len(trace_entries) > _MAX_ENTRIES:
        trace_entries.pop(0)
    if _broadcast_queue is not None:
        _broadcast_queue.put(json.dumps(entry))


def clear():
    trace_entries.clear()
