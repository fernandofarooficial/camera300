import atexit
import json
import os
import queue
import threading
import time

from flask import Flask, jsonify, render_template, request, Response

from tracks import tracks_bp, query_heimdall, HEIMDALL_IMAGE_BASE, get_best_face
import psycopg2.extras
from db import adequar_bases, admin_people
from config import get_faciais_conn, release_faciais_conn, SCORE_MINIMO
import tracer


def get_best_match(track_id):
    """Retorna (image_path, camera_id, face_det_score) do melhor match do Heimdall."""
    # tracer.trace(track_id, 'chamando query_heimdall a partir de get_best_match')
    data, error = query_heimdall(str(track_id))
    if error or not data:
        return None, None, None
    matches = data.get("matches", [])
    for face in matches:
        score_raw = face.get("face_det_score")
        try:
            if score_raw is not None and float(score_raw) >= SCORE_MINIMO:
                path = face.get("image_path")
                if path:
                    return path, face.get("camera_id"), float(score_raw)
        except (ValueError, TypeError):
            continue
    for face in matches:
        path = face.get("image_path")
        if path:
            score_raw = face.get("face_det_score")
            try:
                score = float(score_raw) if score_raw is not None else None
            except (ValueError, TypeError):
                score = None
            return path, face.get("camera_id"), score
    return None, None, None


_RETRY_DELAYS = [3, 12, 20]  # segundos entre tentativas (total: até 3 tentativas)


def salvar_rosto(track_id, camera_id=None, log_id=None, json_record_id=None):
    tracer.trace(track_id, f"salvar_rosto: iniciado (camera_id={camera_id})")
    conn = None
    cursor = None
    try:
        image_path, cam_from_match, face_det_score, face_recgn_score = None, None, None, None
        heimdall_data = None
        for tentativa, delay in enumerate([0] + _RETRY_DELAYS, start=1):
            if delay:
                # tracer.trace(track_id, f"salvar_rosto: aguardando {delay}s antes da tentativa {tentativa}")
                time.sleep(delay)
            heimdall_data, _ = query_heimdall(str(track_id))
            image_path, cam_from_match, face_det_score, face_recgn_score = get_best_face(track_id, data=heimdall_data or {})
            tracer.trace(track_id, f"salvar_rosto: tentativa {tentativa} → image_path={image_path} score={face_det_score} recgn={face_recgn_score}")
            if face_det_score is not None and face_det_score >= SCORE_MINIMO:
                break
        else:
            _processed_no_face.add(track_id)
            return

        # camera_id do match tem prioridade; usa o do payload como fallback
        camera_id = cam_from_match or camera_id
        conn = get_faciais_conn()
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO detection_records
               (track_id, camera_id, image_path, detection_score, recognition_score, log_id, json_record_id, store_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (track_id, camera_id, image_path, face_det_score, face_recgn_score, log_id, json_record_id, 1),
        )
        conn.commit()
        admin_people(track_id, data=heimdall_data)
        # tracer.trace(track_id, "salvar_rosto: foi chamada a função administrar pessoas")
    except Exception as e:
        # tracer.trace(track_id, f"salvar_rosto: ERRO → {e}")
        print(f"Erro ao salvar no banco: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_faciais_conn(conn)


app = Flask(__name__)
app.register_blueprint(tracks_bp)

events = []
listeners = []
event_queue = queue.Queue()
tracer.set_queue(event_queue)
last_seen_track: dict = {}  # track_id -> timestamp do último evento exibido
DEDUP_SECONDS = 5
_processed_no_face: set = set()  # track_ids processados sem face válida

_events_lock = threading.Lock()
_listeners_lock = threading.Lock()
_dedup_lock = threading.Lock()


def broadcaster():
    while True:
        try:
            msg = event_queue.get(timeout=1)
            with _listeners_lock:
                snapshot = listeners[:]
            for q in snapshot:
                q.put(msg)
        except queue.Empty:
            pass


threading.Thread(target=broadcaster, daemon=True).start()


def _iniciar_scheduler():
    """
    Inicia o APScheduler com a agenda de sincronização Microvix.

    Agenda:
      - Seg-Sex: 07:35 às 18:35 (12 execuções por dia)
      - Sáb:     07:35 às 13:35  (7 execuções)

    Usa um lock de arquivo para garantir que apenas UM processo inicie o
    scheduler, evitando disparos duplicados em ambientes gunicorn multi-worker.
    """
    import tempfile

    lock_path = os.path.join(tempfile.gettempdir(), 'toma_na_cara_scheduler.lock')
    try:
        fd = open(lock_path, 'w')
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        # Outro worker gunicorn já iniciou o scheduler.
        return

    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from microvix_ingest import run_incremental, get_status

    def _disparar_sync():
        st = get_status()
        if st['running']:
            tracer.trace("SCHEDULER", "Microvix sync ignorado — já em andamento")
            return
        tracer.trace("SCHEDULER", "Microvix sync iniciado pelo agendador")
        threading.Thread(target=run_incremental, daemon=True).start()

    scheduler = BackgroundScheduler(timezone='America/Sao_Paulo')

    # Segunda a sexta: 07:35 até 18:35
    scheduler.add_job(
        _disparar_sync,
        CronTrigger(day_of_week='mon-fri', hour='7-18', minute=35,
                    timezone='America/Sao_Paulo'),
        id='microvix_seg_sex',
        name='Microvix Seg-Sex',
    )

    # Sábado: 07:35 até 13:35
    scheduler.add_job(
        _disparar_sync,
        CronTrigger(day_of_week='sat', hour='7-13', minute=35,
                    timezone='America/Sao_Paulo'),
        id='microvix_sab',
        name='Microvix Sab',
    )

    scheduler.start()
    atexit.register(scheduler.shutdown)
    tracer.trace("SCHEDULER", "APScheduler iniciado (Seg-Sex 07:35-18:35 | Sáb 07:35-13:35)")


_iniciar_scheduler()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/service-worker.js')
def service_worker():
    response = app.send_static_file('service-worker.js')
    response.headers['Content-Type'] = 'application/javascript'
    response.headers['Service-Worker-Allowed'] = '/'
    response.headers['Cache-Control'] = 'no-cache'
    return response


@app.route('/api/data/facial_recognition', methods=['POST'])
def receive_facial_recognition():
    # tracer.trace("receive_facial_recognition", "Iniciado tratamento do evento, json gravado")
    payload = request.get_json(silent=True, force=True)
    if payload is None:
        return jsonify({'error': 'Invalid or missing JSON body'}), 400

    log_id = payload.get('log_id') or payload.get('data', {}).get('log_id')
    trk_id = payload.get('track_id') or payload.get('data', {}).get('track_id')
    score_post = payload.get('score') or payload.get('data', {}).get('score')

    tracer.trace("Recebi Evento",trk_id)
    tracer.trace(trk_id, f"score post: {score_post}")

    json_record_id = None
    try:
        _conn = get_faciais_conn()
        _cur = _conn.cursor()
        payload_to_save = {**payload}
        if 'image_base64' in payload_to_save:
            payload_to_save['image_base64'] = 'foto'
        if 'data' in payload_to_save and isinstance(payload_to_save['data'], dict) and 'image_base64' in payload_to_save['data']:
            payload_to_save['data'] = {**payload_to_save['data'], 'image_base64': 'foto'}
        _cur.execute(
            "INSERT INTO json_records (log_id, payload) VALUES (%s, %s) RETURNING json_record_id",
            (log_id, json.dumps(payload_to_save)),
        )
        json_record_id = _cur.fetchone()[0]
        _conn.commit()
        _cur.close()
        release_faciais_conn(_conn)
    except Exception as _e:
        print(f"Erro ao salvar json_records: {_e}")

    track_id = payload.get('data', {}).get('track_id')
    camera_id = payload.get('data', {}).get('camera_id')

    now = time.time()
    with _dedup_lock:
        expired = [k for k, ts in last_seen_track.items() if (now - ts) >= DEDUP_SECONDS]
        for k in expired:
            del last_seen_track[k]
        if track_id is not None:
            last_ts = last_seen_track.get(track_id)
            if last_ts is not None and (now - last_ts) < DEDUP_SECONDS:
                return jsonify({'success': True, 'message': 'Duplicate suppressed'}), 200
            last_seen_track[track_id] = now

    event = {
        'received_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'payload': payload,
    }
    with _events_lock:
        events.insert(0, event)
        if len(events) > 100:
            events.pop()

    event_queue.put(json.dumps(event))

    if track_id is not None:
        threading.Thread(target=salvar_rosto, args=(track_id, camera_id, log_id, json_record_id), daemon=True).start()

    return jsonify({'success': True, 'message': 'Data received'}), 200


@app.route('/stream')
def stream():
    def generate():
        q = queue.Queue()
        with _listeners_lock:
            listeners.append(q)
        try:
            with _events_lock:
                snapshot = list(reversed(events))
            for ev in snapshot:
                yield f"data: {json.dumps(ev)}\n\n"
            while True:
                try:
                    data = q.get(timeout=30)
                    yield f"data: {data}\n\n"
                except queue.Empty:
                    yield ": ping\n\n"
        except GeneratorExit:
            with _listeners_lock:
                if q in listeners:
                    listeners.remove(q)

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/track_image/<track_id>')
def get_track_image(track_id):
    # Verifica banco primeiro
    conn = None
    try:
        conn = get_faciais_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            """SELECT image_path, camera_id, detection_score
               FROM detection_records
               WHERE track_id = %s
               ORDER BY detection_record_id DESC LIMIT 1""",
            (track_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        if row and row['image_path']:
            return jsonify({
                'image_url': HEIMDALL_IMAGE_BASE + row['image_path'],
                'camera_id': row['camera_id'],
                'face_det_score': row.get('detection_score'),
            })
        if row and row['camera_id']:
            return jsonify({'image_url': None, 'camera_id': row['camera_id'], 'face_det_score': row.get('detection_score')})
    except Exception as e:
        print(f"Erro ao buscar do banco: {e}")
    finally:
        if conn:
            release_faciais_conn(conn)

    # Fallback: consulta Heimdall (não tenta se já sabemos que não há face)
    if track_id in _processed_no_face:
        return jsonify({'image_url': None, 'camera_id': None, 'face_det_score': None})
    image_path, camera_id, face_det_score = get_best_match(track_id)
    return jsonify({
        'image_url': HEIMDALL_IMAGE_BASE + image_path if image_path else None,
        'camera_id': camera_id,
        'face_det_score': face_det_score,
    })


@app.route('/events')
def get_events():
    with _events_lock:
        return jsonify(list(events))


@app.route('/clear', methods=['POST'])
def clear_events():
    with _events_lock:
        events.clear()
    tracer.clear()
    return jsonify({'success': True})


@app.route('/api/traces')
def get_traces():
    return jsonify(tracer.trace_entries)


_adequar_rodando = False
_adequar_lock = threading.Lock()


@app.route('/api/adequar_bases', methods=['POST'])
def run_adequar_bases():
    global _adequar_rodando
    with _adequar_lock:
        if _adequar_rodando:
            return jsonify({'success': False, 'message': 'Já em execução'}), 409
        _adequar_rodando = True

    def _run():
        global _adequar_rodando
        try:
            adequar_bases()
        except Exception as e:
            tracer.trace("SISTEMA", f"adequar_bases: ERRO inesperado → {e}")
        finally:
            _adequar_rodando = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'success': True})


@app.route('/api/adequar_bases/status')
def adequar_bases_status():
    return jsonify({'rodando': _adequar_rodando})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)
