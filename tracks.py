import io
import cv2
import requests
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from flask import Blueprint, render_template, jsonify, request, Response, send_file, make_response
from requests_toolbelt import MultipartEncoder
from datetime import datetime, timedelta, date

from config import get_conn, get_pg_conn, release_pg_conn, HEIMDALL_URL, HEIMDALL_IMAGE_BASE, HEIMDALL_START_DATE, HEIMDALL_END_DATE, ZIONS_API_URL, ZIONS_TOKEN
from tracer import trace


def _carregar_cameras():
    """Carrega câmeras válidas da view vw_cameras_completo.

    Retorna:
        camera_ids   — lista de str com os id_camera
        cameras_list — lista completa de dicts retornados pela view
    """
    try:
        conn = get_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM vw_cameras_completo")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        ids = [str(r["id_camera"]) for r in rows if r.get("id_camera") is not None]
        return ids, rows
    except Exception as e:
        print(f"[tracks] Erro ao carregar câmeras do banco: {e}")
        return [], []


# IDs das câmeras a consultar e lista completa carregados da view vw_cameras_completo
CAMERA_IDS, CAMERAS_COMPLETO = _carregar_cameras()

tracks_bp = Blueprint("tracks", __name__)


def get_last_track_ids(limit=5):
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT track_id, id, created_at, id_unico, image_path FROM registros ORDER BY id DESC LIMIT %s",
        (limit,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def query_heimdall(track_id):
    trace(track_id, f"query_heimdall: POST {HEIMDALL_URL} (câmeras: {CAMERA_IDS})")
    try:
        fields = [
            ("track_id",   track_id),
            ("confidence", "0.7"),
            ("start_date", HEIMDALL_START_DATE),
            ("end_date",   HEIMDALL_END_DATE),
        ]
        for cam_id in CAMERA_IDS:
            fields.append(("camera_id", cam_id))

        m = MultipartEncoder(fields=fields)
        resp = requests.post(
            HEIMDALL_URL,
            data=m,
            headers={"Content-Type": m.content_type},
            timeout=15,
        )
        if resp.status_code == 404:
            trace(track_id, "query_heimdall: track_id não encontrado no Heimdall (404) — sem matches")
            return {}, None
        if not resp.ok:
            trace(track_id, f"query_heimdall: ERRO HTTP {resp.status_code}")
            return None, f"HTTP {resp.status_code} — {resp.text[:500]}"
        trace(track_id, f"query_heimdall: resposta OK ({resp.status_code})")
        return resp.json(), None
    except requests.exceptions.RequestException as e:
        trace(track_id, f"query_heimdall: ERRO de rede → {e}")
        return None, str(e)
    except ValueError as e:
        trace(track_id, f"query_heimdall: JSON inválido → {e}")
        return None, f"JSON inválido: {e}"


def build_image_url(camera_id, filename):
    if not camera_id or not filename:
        return None
    return f"{HEIMDALL_IMAGE_BASE}/{camera_id}/{filename}"


@tracks_bp.route("/tracks")
def tracks_page():
    rows = get_last_track_ids(5)
    results = []
    for row in rows:
        track_id = row["track_id"]
        trace(track_id,"Chamando query_heimdall a partir de tracks_page")
        data, error = query_heimdall(track_id)
        results.append({
            "track_id": track_id,
            "db_id": row.get("id"),
            "db_created_at": str(row.get("created_at", "")),
            "data": data,
            "error": error,
            "image_base": HEIMDALL_IMAGE_BASE,
        })
    return render_template("tracks.html", results=results)


def fmt_timestamp(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return value


def fmt_score(value):
    try:
        return f"{float(value) * 100:.1f}%"
    except (ValueError, TypeError):
        return value


GENDER_MAP = {0: "F", 1: "M"}


def get_best_face(track_id, data=None):
    """Retorna (image_path, camera_id, face_det_score) da face com maior score do Heimdall."""
    if data is None:
        trace(track_id, "get_best_face: iniciado e já chamando query_heimdall")
        data, error = query_heimdall(str(track_id))
        if error or not data:
            trace(track_id, f"get_best_face: sem dados do Heimdall → {error}")
            return None, None, None
    else:
        trace(track_id, "get_best_face: iniciado com dados já disponíveis")
    caras = (data.get("track") or {}).get("faces", [])
    trace(track_id, f"get_best_face: {len(caras)} face(s) encontrada(s) em track.faces")
    best = None
    best_score = -1
    for face in caras:
        if not face.get("image_path"):
            continue
        try:
            score = float(face.get("face_det_score") or 0)
        except (ValueError, TypeError):
            score = 0
        if score > best_score:
            best_score = score
            best = face
    if best:
        # face_recgn_score não existe em track.faces[] — está em matches[].
        # Usa o maior score entre todos os matches disponíveis.
        recgn_score = None
        for match in (data.get("matches") or []):
            try:
                s = float(match.get("face_recgn_score") or 0) or None
            except (ValueError, TypeError):
                s = None
            if s is not None and (recgn_score is None or s > recgn_score):
                recgn_score = s
        trace(track_id, f"get_best_face: melhor face → image_path={best['image_path']} camera_id={best.get('camera_id')} score={best_score:.4f} recgn={recgn_score}")
        return best["image_path"], best.get("camera_id"), best_score if best_score >= 0 else None, recgn_score
    trace(track_id, "get_best_face: nenhuma face com image_path encontrada")
    return None, None, None, None


def get_pessoas_by_ids(id_unicos):
    """Retorna dict {id_unico: {nome, apelido}} para os ids fornecidos."""
    ids = [i for i in id_unicos if i is not None]
    if not ids:
        return {}
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    placeholders = ",".join(["%s"] * len(ids))
    cursor.execute(
        f"SELECT id_unico, nome, apelido FROM pessoas WHERE id_unico IN ({placeholders})",
        ids,
    )
    pessoas = {p["id_unico"]: p for p in cursor.fetchall()}
    cursor.close()
    conn.close()
    return pessoas


@tracks_bp.route("/tracks/resumo")
def tracks_resumo():
    rows = get_last_track_ids(30)

    id_unicos = [row.get("id_unico") for row in rows]
    pessoas = get_pessoas_by_ids(id_unicos)

    results = []
    for row in rows:
        track_id = row["track_id"]
        data, error = query_heimdall(track_id)

        faces = []
        if data and isinstance(data, dict):
            matches = data.get("matches", [])
            for face in matches:
                score_raw = face.get("face_det_score")
                try:
                    if score_raw is None or float(score_raw) < 0.73:
                        continue
                except (ValueError, TypeError):
                    continue
                gender_raw = face.get("gender")
                recgn_raw = face.get("face_recgn_score")
                faces.append({
                    "id":               face.get("id"),
                    "track_id":         face.get("track_id", track_id),
                    "age":              face.get("age"),
                    "gender":           GENDER_MAP.get(gender_raw, gender_raw),
                    "face_det_score":   fmt_score(score_raw) if score_raw is not None else None,
                    "face_recgn_score": fmt_score(recgn_raw) if recgn_raw is not None else None,
                    "image_path":       face.get("image_path"),
                    "image_url":        HEIMDALL_IMAGE_BASE + face["image_path"] if face.get("image_path") else None,
                    "timestamp":        fmt_timestamp(face.get("timestamp")),
                })

        id_unico = row.get("id_unico")
        pessoa = pessoas.get(id_unico, {})
        results.append({
            "track_id":   track_id,
            "db_id":      row.get("id"),
            "id_unico":   id_unico,
            "nome":       pessoa.get("nome"),
            "apelido":    pessoa.get("apelido"),
            "image_path": row.get("image_path"),
            "error":      error,
            "faces":      faces,
        })
    return render_template("tracks_resumo.html", results=results)


@tracks_bp.route("/tracks/lista")
def tracks_lista():
    per_page = 5
    page = request.args.get('page', 1, type=int)
    if page < 1:
        page = 1

    # Filtro por id_unico (lista separada por vírgulas, espaços ou quebras de linha)
    ids_raw = request.args.get('ids', '').strip()
    ids_filter = []
    if ids_raw:
        for part in ids_raw.replace('\n', ',').replace(';', ',').split(','):
            part = part.strip()
            if part.isdigit():
                ids_filter.append(int(part))

    offset = (page - 1) * per_page

    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    if ids_filter:
        ph = ",".join(["%s"] * len(ids_filter))
        cursor.execute(
            f"SELECT COUNT(DISTINCT id_unico) AS total FROM registros WHERE id_unico IS NOT NULL AND id_unico IN ({ph})",
            ids_filter,
        )
    else:
        cursor.execute("SELECT COUNT(DISTINCT id_unico) AS total FROM registros WHERE id_unico IS NOT NULL")
    total = cursor.fetchone()["total"]
    total_pages = max(1, (total + per_page - 1) // per_page)

    if ids_filter:
        ph = ",".join(["%s"] * len(ids_filter))
        cursor.execute(f"""
            SELECT id_unico
            FROM registros
            WHERE id_unico IS NOT NULL AND id_unico IN ({ph})
            GROUP BY id_unico
            ORDER BY MAX(created_at) DESC
            LIMIT %s OFFSET %s
        """, ids_filter + [per_page, offset])
    else:
        cursor.execute("""
            SELECT id_unico
            FROM registros
            WHERE id_unico IS NOT NULL
            GROUP BY id_unico
            ORDER BY MAX(created_at) DESC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
    id_unicos = [row["id_unico"] for row in cursor.fetchall()]

    groups = []
    if id_unicos:
        placeholders = ",".join(["%s"] * len(id_unicos))
        cursor.execute(f"""
            SELECT
                r.id, r.track_id, r.id_unico, r.image_path, r.created_at, r.camera_id,
                r.face_det_score,
                p.nome, p.apelido, p.flag, p.genero, p.track_id_base, p.doc, p.idade, p.notas
            FROM registros r
            LEFT JOIN pessoas p ON r.id_unico = p.id_unico
            WHERE r.id_unico IN ({placeholders})
            ORDER BY r.created_at DESC
        """, id_unicos)
        rows = cursor.fetchall()

        groups_dict = {}
        for row in rows:
            key = row["id_unico"]
            if key not in groups_dict:
                groups_dict[key] = {
                    "id_unico":      key,
                    "nome":          row["nome"],
                    "apelido":       row["apelido"],
                    "flag":          row["flag"],
                    "genero":        row["genero"],
                    "doc":           row["doc"],
                    "idade":         row["idade"],
                    "notas":         row["notas"],
                    "track_id_base": row["track_id_base"],
                    "foto":          None,
                    "ocorrencias":   [],
                }
            if groups_dict[key]["foto"] is None and row["image_path"] and row["track_id"] == groups_dict[key]["track_id_base"]:
                groups_dict[key]["foto"] = HEIMDALL_IMAGE_BASE + row["image_path"]
            groups_dict[key]["ocorrencias"].append({
                "id":             row["id"],
                "track_id":       row["track_id"],
                "image_url":      HEIMDALL_IMAGE_BASE + row["image_path"] if row["image_path"] else None,
                "created_at":     fmt_timestamp(row["created_at"]),
                "camera_id":      row["camera_id"],
                "face_det_score": fmt_score(row["face_det_score"]) if row.get("face_det_score") is not None else None,
            })
        groups = [groups_dict[k] for k in id_unicos if k in groups_dict]

    cursor.close()
    conn.close()

    cameras_map = {c["id_camera"]: c.get("camera") or str(c["id_camera"]) for c in CAMERAS_COMPLETO if c.get("id_camera") is not None}
    return render_template("tracks_lista.html", groups=groups, page=page, total_pages=total_pages,
                           cameras_map=cameras_map, ids_raw=ids_raw)


@tracks_bp.route("/tracks/permanencia")
def tracks_permanencia():
    per_page = 5
    page = request.args.get('page', 1, type=int)
    if page < 1:
        page = 1
    offset = (page - 1) * per_page

    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT COUNT(DISTINCT id_unico) AS total FROM registros WHERE id_unico IS NOT NULL")
    total = cursor.fetchone()["total"]
    total_pages = max(1, (total + per_page - 1) // per_page)

    cursor.execute("""
        SELECT id_unico
        FROM registros
        WHERE id_unico IS NOT NULL
        GROUP BY id_unico
        ORDER BY MAX(created_at) DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))
    id_unicos = [row["id_unico"] for row in cursor.fetchall()]

    groups = []
    if id_unicos:
        placeholders = ",".join(["%s"] * len(id_unicos))
        cursor.execute(f"""
            SELECT
                r.id, r.track_id, r.id_unico, r.image_path, r.created_at,
                p.nome, p.apelido, p.flag, p.genero, p.doc, p.idade, p.notas, p.track_id_base
            FROM registros r
            LEFT JOIN pessoas p ON r.id_unico = p.id_unico
            WHERE r.id_unico IN ({placeholders})
            ORDER BY r.created_at ASC
        """, id_unicos)
        rows = cursor.fetchall()

        groups_dict = {}
        for row in rows:
            key = row["id_unico"]
            dia = row["created_at"].date() if row["created_at"] else None

            if key not in groups_dict:
                groups_dict[key] = {
                    "id_unico":      key,
                    "nome":          row["nome"],
                    "apelido":       row["apelido"],
                    "flag":          row["flag"],
                    "genero":        row["genero"],
                    "doc":           row["doc"],
                    "idade":         row["idade"],
                    "notas":         row["notas"],
                    "track_id_base": row["track_id_base"],
                    "foto":          None,
                    "dias":          {},
                }

            g = groups_dict[key]
            if g["foto"] is None and row["image_path"] and row["track_id"] == g["track_id_base"]:
                g["foto"] = HEIMDALL_IMAGE_BASE + row["image_path"]

            if dia not in g["dias"]:
                g["dias"][dia] = []
            g["dias"][dia].append(row)

        for key, g in groups_dict.items():
            dias_list = []
            for dia in sorted(g["dias"].keys(), reverse=True):
                regs = g["dias"][dia]  # já ordenados por created_at ASC
                primeira = regs[0]
                ultima = regs[-1]

                if len(regs) == 1 or (ultima["created_at"] - primeira["created_at"]) < timedelta(minutes=2):
                    duracao = timedelta(minutes=30)
                    estimado = True
                else:
                    duracao = ultima["created_at"] - primeira["created_at"]
                    estimado = False

                total_secs = int(duracao.total_seconds())
                horas = total_secs // 3600
                mins = (total_secs % 3600) // 60
                if horas > 0:
                    perm_str = f"{horas}h {mins:02d}min"
                else:
                    perm_str = f"{mins}min"

                dias_list.append({
                    "dia":       str(dia),
                    "hora_ini":  fmt_timestamp(primeira["created_at"]),
                    "hora_fim":  fmt_timestamp(ultima["created_at"]) if not estimado else None,
                    "permanencia": perm_str,
                    "estimado":  estimado,
                    "image_url": HEIMDALL_IMAGE_BASE + primeira["image_path"] if primeira["image_path"] else None,
                })
            g["dias"] = dias_list

        groups = [groups_dict[k] for k in id_unicos if k in groups_dict]

    cursor.close()
    conn.close()

    return render_template("tracks_permanencia.html", groups=groups, page=page, total_pages=total_pages)


@tracks_bp.route("/tracks/api/pessoa/<int:id_unico>", methods=["POST"])
def atualizar_pessoa(id_unico):
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "JSON inválido"}), 400
    campos_permitidos = {"nome", "apelido", "doc", "idade", "genero", "flag", "notas"}
    updates = {k: (v if v != "" else None) for k, v in data.items() if k in campos_permitidos}
    if not updates:
        return jsonify({"error": "Nenhum campo válido"}), 400
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [id_unico]
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE pessoas SET {set_clause} WHERE id_unico = %s", values)
        conn.commit()
        cursor.close()
    finally:
        conn.close()
    return jsonify({"success": True})


@tracks_bp.route("/tracks/api/pessoa/<int:id_unico>/base", methods=["POST"])
def atualizar_base_pessoa(id_unico):
    data = request.get_json(silent=True)
    if not data or "track_id_base" not in data:
        return jsonify({"error": "track_id_base obrigatório"}), 400
    track_id_base = data["track_id_base"]
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE pessoas SET track_id_base = %s WHERE id_unico = %s", (track_id_base, id_unico))
        conn.commit()
        cursor.close()
    finally:
        conn.close()
    return jsonify({"success": True})


@tracks_bp.route("/tracks/api/pessoa/<int:id_unico>", methods=["DELETE"])
def excluir_pessoa(id_unico):
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM registros WHERE id_unico = %s", (id_unico,))
        cursor.execute("DELETE FROM pessoas WHERE id_unico = %s", (id_unico,))
        conn.commit()
        cursor.close()
    finally:
        conn.close()
    return jsonify({"success": True})


@tracks_bp.route("/tracks/api/pessoa/<int:id_unico>", methods=["GET"])
def buscar_pessoa(id_unico):
    conn = get_conn()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT p.id_unico, p.nome, p.apelido, p.flag,
                   (SELECT r.image_path FROM registros r
                    WHERE r.track_id = p.track_id_base AND r.image_path IS NOT NULL
                    ORDER BY r.face_det_score DESC LIMIT 1) AS image_path
            FROM pessoas p
            WHERE p.id_unico = %s
            """,
            (id_unico,),
        )
        pessoa = cursor.fetchone()
        cursor.close()
    finally:
        conn.close()
    if pessoa is None:
        return jsonify({"error": f"Pessoa {id_unico} não encontrada"}), 404
    pessoa["foto"] = HEIMDALL_IMAGE_BASE + pessoa["image_path"] if pessoa.get("image_path") else None
    del pessoa["image_path"]
    return jsonify(pessoa)


@tracks_bp.route("/tracks/api/registro/<int:reg_id>", methods=["DELETE"])
def excluir_registro(reg_id):
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM registros WHERE id = %s", (reg_id,))
        conn.commit()
        cursor.close()
    finally:
        conn.close()
    return jsonify({"success": True})


@tracks_bp.route("/tracks/api/registro/<int:reg_id>/mover", methods=["POST"])
def mover_registro(reg_id):
    data = request.get_json(silent=True)
    if not data or "id_unico" not in data:
        return jsonify({"error": "id_unico obrigatório"}), 400
    id_unico = data["id_unico"]
    conn = get_conn()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id_unico FROM pessoas WHERE id_unico = %s", (id_unico,))
        if cursor.fetchone() is None:
            cursor.close()
            return jsonify({"error": f"Pessoa {id_unico} não encontrada"}), 404

        # descobre o id_unico original do registro antes de mover
        cursor.execute("SELECT id_unico FROM registros WHERE id = %s", (reg_id,))
        row = cursor.fetchone()
        id_unico_original = row["id_unico"] if row else None

        cursor.execute("UPDATE registros SET id_unico = %s WHERE id = %s", (id_unico, reg_id))

        # se havia um dono original, verifica se ainda tem registros; se não, exclui a pessoa
        pessoa_excluida = False
        if id_unico_original and id_unico_original != id_unico:
            cursor.execute("SELECT COUNT(*) AS total FROM registros WHERE id_unico = %s", (id_unico_original,))
            if cursor.fetchone()["total"] == 0:
                cursor.execute("DELETE FROM pessoas WHERE id_unico = %s", (id_unico_original,))
                pessoa_excluida = True

        conn.commit()
        cursor.close()
    finally:
        conn.close()
    return jsonify({"success": True, "pessoa_excluida": pessoa_excluida})


@tracks_bp.route("/tracks/tabuleiro")
def tracks_tabuleiro():
    per_page = 30
    page = request.args.get('page', 1, type=int)
    if page < 1:
        page = 1
    offset = (page - 1) * per_page

    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT COUNT(*) AS total FROM pessoas")
    total = cursor.fetchone()["total"]
    total_pages = max(1, (total + per_page - 1) // per_page)

    cursor.execute("""
        SELECT
            p.id_unico,
            p.nome,
            p.apelido,
            p.flag,
            p.genero,
            p.doc,
            p.idade,
            p.notas,
            (SELECT r.image_path FROM registros r
             WHERE r.track_id = p.track_id_base AND r.image_path IS NOT NULL
             ORDER BY r.face_det_score DESC LIMIT 1) AS image_path
        FROM pessoas p
        ORDER BY p.id_unico DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))
    pessoas = cursor.fetchall()

    cursor.close()
    conn.close()

    for p in pessoas:
        p["foto"] = HEIMDALL_IMAGE_BASE + p["image_path"] if p["image_path"] else None

    return render_template("tracks_tabuleiro.html", pessoas=pessoas, page=page, total_pages=total_pages)


@tracks_bp.route("/tracks/quadro")
def tracks_quadro():
    from datetime import date, timedelta
    today = date.today()
    week_ago = today - timedelta(days=7)

    # Cada seção do quadro tem seu próprio filtro de período de data:
    #   d_ini/d_fim → gráfico de ocorrências por dia
    #   h_ini/h_fim → gráfico de ocorrências por hora (filtro de datas, não de horas)
    #   p_ini/p_fim → gráfico de ocorrências por pessoa
    #   e_ini/e_fim → gráfico de ocorrências por faixa etária (clientes)
    d_ini = request.args.get('d_ini', week_ago.isoformat())
    d_fim = request.args.get('d_fim', today.isoformat())
    h_ini = request.args.get('h_ini', week_ago.isoformat())
    h_fim = request.args.get('h_fim', today.isoformat())
    p_ini = request.args.get('p_ini', week_ago.isoformat())
    p_fim = request.args.get('p_fim', today.isoformat())
    e_ini = request.args.get('e_ini', week_ago.isoformat())
    e_fim = request.args.get('e_fim', today.isoformat())
    perm_ini = request.args.get('perm_ini', week_ago.isoformat())
    perm_fim = request.args.get('perm_fim', today.isoformat())
    gd_ini = request.args.get('gd_ini', week_ago.isoformat())
    gd_fim = request.args.get('gd_fim', today.isoformat())
    fat_ini = request.args.get('fat_ini', week_ago.isoformat())
    fat_fim = request.args.get('fat_fim', today.isoformat())

    # ── Faturamento diário (PostgreSQL) ───────────────────────────────────────
    faturamento_por_dia = []
    pg_conn = None
    try:
        pg_conn = get_pg_conn()
        pg_cur = pg_conn.cursor()
        pg_cur.execute("""
            SELECT
                data_lancamento::date AS data_doc,
                COUNT(DISTINCT documento) AS qtd_notas,
                SUM(valor_total) AS soma
            FROM microvix_movimento
            WHERE cod_natureza_operacao = '10030'
              AND cancelado = 'N'
              AND excluido = 'N'
              AND data_lancamento::date BETWEEN %s AND %s
            GROUP BY data_lancamento::date
            ORDER BY data_lancamento::date ASC
        """, (fat_ini, fat_fim))
        for row in pg_cur.fetchall():
            faturamento_por_dia.append({
                "dia": str(row[0]),
                "qtd_notas": int(row[1]),
                "soma": float(row[2]) if row[2] is not None else 0.0,
                "qtd_clientes": 0,
            })
        pg_cur.close()
    except Exception as e:
        print(f"[quadro] Erro PostgreSQL faturamento: {e}")
    finally:
        if pg_conn:
            release_pg_conn(pg_conn)

    # ── Clientes por dia no intervalo do faturamento (MySQL) ─────────────────
    if faturamento_por_dia:
        try:
            _conn_fat = get_conn()
            _cur_fat = _conn_fat.cursor(dictionary=True)
            _cur_fat.execute("""
                SELECT DATE(r.created_at) AS dia, COUNT(DISTINCT r.id_unico) AS total
                FROM registros r
                JOIN pessoas p ON r.id_unico = p.id_unico
                WHERE DATE(r.created_at) BETWEEN %s AND %s
                  AND p.flag IN ('C', 'A')
                GROUP BY DATE(r.created_at)
            """, (fat_ini, fat_fim))
            clientes_map = {str(row["dia"]): row["total"] for row in _cur_fat.fetchall()}
            _cur_fat.close()
            _conn_fat.close()
            for entry in faturamento_por_dia:
                entry["qtd_clientes"] = clientes_map.get(entry["dia"], 0)
        except Exception as e:
            print(f"[quadro] Erro MySQL clientes/faturamento: {e}")

    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            visits.dia_visit AS dia,
            SUM(CASE WHEN min_global.primeira = visits.dia_visit THEN 1 ELSE 0 END) AS novos,
            SUM(CASE WHEN min_global.primeira < visits.dia_visit THEN 1 ELSE 0 END) AS retornantes
        FROM (
            SELECT DISTINCT r.id_unico, DATE(r.created_at) AS dia_visit
            FROM registros r
            JOIN pessoas p ON r.id_unico = p.id_unico
            WHERE DATE(r.created_at) BETWEEN %s AND %s
              AND p.flag IN ('C', 'A')
        ) AS visits
        JOIN (
            SELECT r.id_unico, MIN(DATE(r.created_at)) AS primeira
            FROM registros r
            JOIN pessoas p ON r.id_unico = p.id_unico
            WHERE p.flag IN ('C', 'A')
            GROUP BY r.id_unico
        ) AS min_global ON visits.id_unico = min_global.id_unico
        GROUP BY visits.dia_visit
        ORDER BY visits.dia_visit ASC
    """, (d_ini, d_fim))
    rows = cursor.fetchall()
    ocorrencias_por_dia = [
        {"dia": str(r["dia"]), "novos": r["novos"], "retornantes": r["retornantes"]}
        for r in rows
    ]

    cursor.execute("""
        SELECT HOUR(primeira.created_at) AS hora, COUNT(*) AS total
        FROM (
            SELECT r.id_unico, DATE(r.created_at) AS dia, MIN(r.created_at) AS created_at
            FROM registros r
            JOIN pessoas p ON r.id_unico = p.id_unico
            WHERE DATE(r.created_at) BETWEEN %s AND %s
              AND p.flag IN ('C', 'A')
            GROUP BY r.id_unico, DATE(r.created_at)
        ) AS primeira
        GROUP BY HOUR(primeira.created_at)
        ORDER BY hora ASC
    """, (h_ini, h_fim))
    rows_hora = cursor.fetchall()

    cursor.execute("""
        SELECT p.apelido, COUNT(DISTINCT DATE(r.created_at)) AS total
        FROM registros r
        JOIN pessoas p ON r.id_unico = p.id_unico
        WHERE DATE(r.created_at) BETWEEN %s AND %s
          AND p.flag IN ('C', 'A')
        GROUP BY r.id_unico, p.apelido
        HAVING COUNT(DISTINCT DATE(r.created_at)) >= 3
        ORDER BY total DESC
    """, (p_ini, p_fim))
    rows_pessoa = cursor.fetchall()

    cursor.execute("""
        SELECT
          CASE
            WHEN p.idade BETWEEN 0  AND 25 THEN '00-25'
            WHEN p.idade BETWEEN 26 AND 35 THEN '26-35'
            WHEN p.idade BETWEEN 36 AND 45 THEN '36-45'
            WHEN p.idade BETWEEN 46 AND 55 THEN '46-55'
            WHEN p.idade BETWEEN 56 AND 65 THEN '56-65'
            WHEN p.idade > 65              THEN '65+'
          END AS faixa,
          COUNT(*) AS total
        FROM (
            SELECT DISTINCT r.id_unico, DATE(r.created_at) AS dia
            FROM registros r
            JOIN pessoas p ON r.id_unico = p.id_unico
            WHERE DATE(r.created_at) BETWEEN %s AND %s
              AND p.flag = 'C'
              AND p.idade IS NOT NULL
        ) AS uniq
        JOIN pessoas p ON uniq.id_unico = p.id_unico
        GROUP BY faixa
        ORDER BY FIELD(faixa, '00-25', '26-35', '36-45', '46-55', '56-65', '65+')
    """, (e_ini, e_fim))
    rows_etaria = cursor.fetchall()

    cursor.execute("""
        SELECT r.id_unico, p.genero,
               MIN(r.created_at) AS primeira, MAX(r.created_at) AS ultima
        FROM registros r
        JOIN pessoas p ON r.id_unico = p.id_unico
        WHERE DATE(r.created_at) BETWEEN %s AND %s
          AND p.flag IN ('C', 'A')
        GROUP BY r.id_unico, DATE(r.created_at), p.genero
    """, (perm_ini, perm_fim))
    rows_perm = cursor.fetchall()

    cursor.execute("""
        SELECT DATE(r.created_at) AS dia, p.genero, COUNT(DISTINCT r.id_unico) AS total
        FROM registros r
        JOIN pessoas p ON r.id_unico = p.id_unico
        WHERE DATE(r.created_at) BETWEEN %s AND %s
          AND p.flag IN ('C', 'A')
          AND p.genero IN ('M', 'F')
        GROUP BY DATE(r.created_at), p.genero
        ORDER BY dia ASC
    """, (gd_ini, gd_fim))
    rows_genero_dia = cursor.fetchall()

    cursor.close()
    conn.close()

    def _perm_minutos(primeira, ultima):
        diff = ultima - primeira
        if diff < timedelta(minutes=2):
            return 30.0
        return diff.total_seconds() / 60

    def _fmt_media(vals):
        if not vals:
            return "—"
        avg = sum(vals) / len(vals)
        h = int(avg) // 60
        m = int(avg) % 60
        return f"{h}h {m:02d}min" if h > 0 else f"{m}min"

    perms_total, perms_M, perms_F = [], [], []
    for row in rows_perm:
        mins = _perm_minutos(row["primeira"], row["ultima"])
        perms_total.append(mins)
        if row["genero"] == "M":
            perms_M.append(mins)
        elif row["genero"] == "F":
            perms_F.append(mins)

    def _avg(vals):
        return round(sum(vals) / len(vals), 1) if vals else 0

    permanencia_media = [
        {"label": "Total",     "minutos": _avg(perms_total), "fmt": _fmt_media(perms_total)},
        {"label": "Masculino", "minutos": _avg(perms_M),     "fmt": _fmt_media(perms_M)},
        {"label": "Feminino",  "minutos": _avg(perms_F),     "fmt": _fmt_media(perms_F)},
    ]

    gd_map = {}
    for row in rows_genero_dia:
        dia = str(row["dia"])
        if dia not in gd_map:
            gd_map[dia] = {"M": 0, "F": 0}
        gd_map[dia][row["genero"]] = row["total"]
    genero_por_dia = [
        {"dia": d, "M": gd_map[d]["M"], "F": gd_map[d]["F"]}
        for d in sorted(gd_map)
    ]

    horas_map = {r["hora"]: r["total"] for r in rows_hora}
    ocorrencias_por_hora = [
        {"faixa": "00h00-07h59", "total": sum(horas_map.get(h, 0) for h in range(0, 8))},
        *[{"faixa": f"{h:02d}h00-{h:02d}h59", "total": horas_map.get(h, 0)} for h in range(8, 19)],
        {"faixa": "19h00-21h59", "total": sum(horas_map.get(h, 0) for h in range(19, 22))},
        {"faixa": "22h00-23h59", "total": sum(horas_map.get(h, 0) for h in range(22, 24))},
    ]
    ocorrencias_por_pessoa = [
        {"apelido": r["apelido"], "total": r["total"]}
        for r in rows_pessoa
    ]
    faixas_ordem = ['00-25', '26-35', '36-45', '46-55', '56-65', '65+']
    etaria_map = {r["faixa"]: r["total"] for r in rows_etaria}
    ocorrencias_por_faixa_etaria = [
        {"faixa": f, "total": etaria_map.get(f, 0)}
        for f in faixas_ordem
    ]

    return render_template("tracks_quadro.html",
                           ocorrencias_por_dia=ocorrencias_por_dia,
                           ocorrencias_por_hora=ocorrencias_por_hora,
                           ocorrencias_por_pessoa=ocorrencias_por_pessoa,
                           ocorrencias_por_faixa_etaria=ocorrencias_por_faixa_etaria,
                           permanencia_media=permanencia_media,
                           faturamento_por_dia=faturamento_por_dia,
                           d_ini=d_ini, d_fim=d_fim,
                           h_ini=h_ini, h_fim=h_fim,
                           p_ini=p_ini, p_fim=p_fim,
                           e_ini=e_ini, e_fim=e_fim,
                           perm_ini=perm_ini, perm_fim=perm_fim,
                           genero_por_dia=genero_por_dia,
                           gd_ini=gd_ini, gd_fim=gd_fim,
                           fat_ini=fat_ini, fat_fim=fat_fim)


@tracks_bp.route("/tracks/dados")
def tracks_dados():
    _, cameras = _carregar_cameras()
    return render_template("tracks_dados.html", cameras=cameras)


@tracks_bp.route("/tracks/snapshot/<int:camera_id>")
def tracks_snapshot(camera_id):
    cam = next((c for c in CAMERAS_COMPLETO if c.get("id_camera") == camera_id), None)
    if cam is None or not cam.get("rstp"):
        return Response("Câmera não encontrada ou sem URL RTSP.", status=404, mimetype="text/plain")

    rtsp_url = cam["rstp"]
    cap = cv2.VideoCapture(rtsp_url)
    try:
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        ret, frame = cap.read()
        if not ret or frame is None:
            return Response("Não foi possível capturar frame.", status=502, mimetype="text/plain")
        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
        if not ok:
            return Response("Erro ao codificar imagem.", status=500, mimetype="text/plain")
        return Response(io.BytesIO(buf.tobytes()).read(), mimetype="image/jpeg")
    finally:
        cap.release()


@tracks_bp.route("/tracks/api")
def tracks_api():
    rows = get_last_track_ids(5)
    results = []
    for row in rows:
        track_id = row["track_id"]
        data, error = query_heimdall(track_id)
        results.append({
            "track_id": track_id,
            "data": data,
            "error": error,
        })
    return jsonify(results)


_GENERO_LABEL = {"M": "Masculino", "F": "Feminino", "A": "Anônimo"}
_FLAG_LABEL   = {"C": "Cliente",   "A": "Anônimo",  "F": "Franqueado", "E": "Empregado", "K": "Criança", "P": "Prestador"}


@tracks_bp.route("/tracks/export")
def tracks_export():
    today     = date.today()
    week_ago  = today - timedelta(days=7)
    data_ini  = request.args.get("data_ini", week_ago.isoformat())
    data_fim  = request.args.get("data_fim", today.isoformat())
    return render_template("tracks_export.html", data_ini=data_ini, data_fim=data_fim)


@tracks_bp.route("/tracks/export/download")
def tracks_export_download():
    today    = date.today()
    week_ago = today - timedelta(days=7)
    data_ini = request.args.get("data_ini", week_ago.isoformat())
    data_fim = request.args.get("data_fim", today.isoformat())

    try:
        conn = get_conn()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT
                    r.id,
                    r.camera_id,
                    r.created_at,
                    r.id_unico,
                    p.nome,
                    p.apelido,
                    p.idade,
                    p.genero,
                    p.flag,
                    tc.tipo_camera,
                    c.camera,
                    e.empresa
                FROM registros r
                LEFT JOIN pessoas      p  ON p.id_unico       = r.id_unico
                LEFT JOIN cameras      c  ON c.id_camera       = r.camera_id
                LEFT JOIN tipos_camera tc ON tc.id_tipo_camera = c.id_tipo_camera
                LEFT JOIN locais       l  ON l.id_local        = c.id_local
                LEFT JOIN empresas     e  ON e.id_empresa      = l.id_empresa
                WHERE DATE(r.created_at) BETWEEN %s AND %s
                ORDER BY r.created_at
            """, (data_ini, data_fim))
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()
    except Exception as e:
        print(f"[export] Erro no banco: {e}")
        return Response(f"Erro ao consultar banco de dados: {e}", status=500, mimetype="text/plain")

    try:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Registros"

        header_fill = PatternFill(patternType="solid", fgColor="1F3864")
        header_font = Font(bold=True, color="FFFFFF")
        headers = ["Ocor", "Id Cam", "DataHora", "Id Pessoa",
                   "Nome", "Apelido", "Idade", "Gênero", "Flag",
                   "Tipo Câmera", "Nome Câmera", "Empresa"]
        col_widths = [8, 8, 20, 10, 25, 15, 8, 12, 12, 15, 20, 25]

        for col_idx, (header, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center")
            ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = width

        for row_idx, row in enumerate(rows, start=2):
            ws.cell(row=row_idx, column=1,  value=row["id"])
            ws.cell(row=row_idx, column=2,  value=row["camera_id"])
            ws.cell(row=row_idx, column=3,  value=row["created_at"].strftime("%d/%m/%Y %H:%M:%S") if row["created_at"] else "")
            ws.cell(row=row_idx, column=4,  value=row["id_unico"])
            ws.cell(row=row_idx, column=5,  value=row["nome"] or "")
            ws.cell(row=row_idx, column=6,  value=row["apelido"] or "")
            ws.cell(row=row_idx, column=7,  value=row["idade"])
            ws.cell(row=row_idx, column=8,  value=_GENERO_LABEL.get(row["genero"], row["genero"] or ""))
            ws.cell(row=row_idx, column=9,  value=_FLAG_LABEL.get(row["flag"], row["flag"] or ""))
            ws.cell(row=row_idx, column=10, value=row["tipo_camera"] or "")
            ws.cell(row=row_idx, column=11, value=row["camera"] or "")
            ws.cell(row=row_idx, column=12, value=row["empresa"] or "")

        output = io.BytesIO()
        wb.save(output)
        xlsx_bytes = output.getvalue()
    except Exception as e:
        print(f"[export] Erro ao gerar planilha: {e}")
        return Response(f"Erro ao gerar planilha: {e}", status=500, mimetype="text/plain")

    filename = f"registros_{data_ini}_{data_fim}.xlsx"
    resp = make_response(xlsx_bytes)
    resp.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    resp.headers["Content-Length"] = len(xlsx_bytes)
    return resp


@tracks_bp.route("/tracks/logs")
def tracks_logs():
    error = None
    logs = []
    try:
        resp = requests.get(
            f"{ZIONS_API_URL}/facial_recognition_log",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {ZIONS_TOKEN}",
                "paginate": "10",
                "orderDirection": "desc",
            },
            timeout=10,
        )
        if resp.ok:
            body = resp.json()
            logs = body.get("data", [])
        else:
            error = f"HTTP {resp.status_code}: {resp.text[:300]}"
    except Exception as e:
        error = str(e)
    return render_template("tracks_logs.html", logs=logs, error=error)
