import psycopg2.extras

from tracks import query_heimdall, fmt_timestamp, get_best_face, GENDER_MAP
from config import get_faciais_conn, release_faciais_conn, FLAG_NOVO_ANONIMO, SCORE_MINIMO
from tracer import trace
from telegram import enviar_mensagem_telegram


def adequar_bases():
    trace("SISTEMA", "adequar_bases: iniciado — buscando registros sem image_path")
    conn = get_faciais_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        "SELECT * FROM detection_records WHERE image_path IS NULL ORDER BY detection_record_id LIMIT 200"
    )
    linhas = cursor.fetchall()
    cursor.close()
    release_faciais_conn(conn)
    total = len(linhas)
    trace("SISTEMA", f"adequar_bases: {total} registro(s) encontrado(s) para tratar")
    for e, lin in enumerate(linhas):
        track_id = lin["track_id"]
        idreg = lin["detection_record_id"]
        trace("SISTEMA", f"adequar_bases: [{e + 1}/{total}] track_id={track_id} id={idreg}")
        atualizar_path_camera(track_id, idreg)
        admin_people(track_id)
    trace("SISTEMA", f"adequar_bases: concluído — {total} registro(s) tratado(s)")


def atualizar_path_camera(track_id, idreg):
    image_path, camera_id, face_det_score, face_recgn_score = get_best_face(track_id)
    if image_path is not None:
        conn = get_faciais_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE detection_records
                   SET image_path = %s, camera_id = %s, detection_score = %s, recognition_score = %s
                   WHERE detection_record_id = %s""",
                (image_path, camera_id, face_det_score, face_recgn_score, idreg),
            )
            conn.commit()
            cursor.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            release_faciais_conn(conn)


def admin_people(track_id, data=None):
    faces = listar_matches_simples(track_id, data=data)
    person_id = None
    for caretas in faces:
        track_careta = caretas.get('track_id')
        person_id = obter_person_id_legado(track_careta)
        if person_id is not None:
            atualizar_person_id_em_registro(person_id, track_id)
            recgn_score = caretas.get('face_recgn_score')
            if recgn_score is not None:
                atualizar_recognition_score(track_id, recgn_score)
                trace(track_id, f"admin_people: recognition_score={recgn_score:.4f} atualizado para track_id={track_id}")
            trace(track_id, f"admin_people: person_id={person_id} vinculado ao track_id={track_id}")
            telegram_cliente_chegou(track_id, person_id)
            break
    if person_id is None:
        trace(track_id, "admin_people: nenhum person_id encontrado — criando nova pessoa")
        criar_pessoa(track_id, data=data)
    return True


def listar_matches_simples(track_id, data=None):
    if data is None:
        data, error = query_heimdall(str(track_id))
    else:
        trace(track_id, "listar_matches_simples: usando dados já disponíveis")
    faces = []
    if data and isinstance(data, dict):
        matches = data.get("matches", [])
        for face in matches:
            score_raw = face.get("face_det_score")
            try:
                if score_raw is None or float(score_raw) < SCORE_MINIMO:
                    continue
            except (ValueError, TypeError):
                continue
            recgn_raw = face.get("face_recgn_score")
            try:
                recgn_score = float(recgn_raw) if recgn_raw is not None else None
            except (ValueError, TypeError):
                recgn_score = None
            faces.append({
                "track_id": face.get("track_id", track_id),
                "image_path": face.get("image_path"),
                "timestamp": fmt_timestamp(face.get("timestamp")),
                "face_recgn_score": recgn_score,
            })
    return faces


def obter_person_id_legado(track_id):
    conn = get_faciais_conn()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT person_id FROM detection_records WHERE track_id = %s ORDER BY detection_record_id DESC LIMIT 1",
            (track_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            return None
        return row.get('person_id')
    finally:
        release_faciais_conn(conn)


def atualizar_person_id_em_registro(person_id, track_id):
    conn = get_faciais_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE detection_records SET person_id = %s WHERE track_id = %s",
            (person_id, track_id),
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_faciais_conn(conn)


def atualizar_recognition_score(track_id, recognition_score):
    conn = get_faciais_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE detection_records SET recognition_score = %s WHERE track_id = %s",
            (recognition_score, track_id),
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_faciais_conn(conn)


def criar_pessoa(track_id, data=None):
    gender_id = None
    idade = None
    if data:
        caras = (data.get("track") or {}).get("faces", [])
        for face in caras:
            if gender_id is None and face.get("gender") is not None:
                gender_id = GENDER_MAP.get(face.get("gender"))
            if idade is None and face.get("age") is not None:
                try:
                    idade = int(face.get("age"))
                except (ValueError, TypeError):
                    pass
            if gender_id is not None and idade is not None:
                break
        trace(track_id, f"criar_pessoa: gender_id={gender_id} idade={idade} extraídos do Heimdall")

    conn = get_faciais_conn()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "INSERT INTO people (reference_track_id, person_type_id) VALUES (%s, %s) RETURNING person_id",
            (track_id, FLAG_NOVO_ANONIMO),
        )
        res = cursor.fetchone()
        conn.commit()
        if res is None:
            cursor.close()
            return
        iu = res['person_id']
        full_name = f"Anônimo{iu:03}"
        nickname = f"A{iu:03}"
        trace(track_id, f"criar_pessoa: nova pessoa criada → person_id={iu} nome={full_name}")
        cursor.execute(
            "UPDATE people SET full_name = %s, nickname = %s, gender_id = %s, age = %s WHERE person_id = %s",
            (full_name, nickname, gender_id, idade, iu),
        )
        conn.commit()
        cursor.execute(
            "UPDATE detection_records SET person_id = %s WHERE track_id = %s",
            (iu, track_id),
        )
        conn.commit()
        trace(track_id, f"criar_pessoa: detection_records atualizado com person_id={iu}")
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_faciais_conn(conn)


def telegram_cliente_chegou(track_id, person_id):
    conn = get_faciais_conn()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM people WHERE person_id = %s LIMIT 1", (person_id,))
        pes = cursor.fetchone()
        if pes is None:
            cursor.close()
            return
        if pes.get("person_type_id") == "C":
            cursor.execute(
                """SELECT COUNT(*) AS qtd_hoje
                   FROM detection_records
                   WHERE person_id = %s AND DATE(created_at) = CURRENT_DATE""",
                (person_id,),
            )
            qtd_hoje = cursor.fetchone().get("qtd_hoje", 0)
            if qtd_hoje > 1:
                trace(track_id, f"telegram_cliente_chegou: não é a primeira ocorrência do dia (qtd_hoje={qtd_hoje}) — abortando")
                cursor.close()
                return
            nome = pes.get("full_name")
            nick = pes.get("nickname")
            nota = pes.get("notes")
            cursor.execute(
                "SELECT COUNT(DISTINCT DATE(created_at)) AS qtd_visit FROM detection_records WHERE person_id = %s",
                (person_id,),
            )
            qtd_visit = cursor.fetchone().get("qtd_visit")
            if qtd_visit >= 2:
                cursor.execute(
                    """SELECT created_at AS data_hora
                       FROM detection_records
                       WHERE person_id = %s
                       ORDER BY created_at DESC
                       LIMIT 2""",
                    (person_id,),
                )
                visitas = cursor.fetchall()
                ult_visit = visitas[0]["data_hora"].strftime("%d/%m/%Y %H:%M")
                ant_visit = visitas[1]["data_hora"].strftime("%d/%m/%Y %H:%M")
                msg1 = f"Cliente {nome}({nick}) chegou em {ult_visit}. "
                msg2 = f"Esta é a visita de número {qtd_visit} e visita anterior foi em {ant_visit}. "
                msg3 = f"Nota: {nota}" if nota is not None else "  "
                mensagem = msg1 + msg2 + msg3
            else:
                mensagem = f"Cliente {nome}({nick}) chegou. Esta é a sua primeira visita."
            trace(track_id, f"telegram_cliente_chegou: enviando mensagem para {nome}({nick})")
            enviar_mensagem_telegram(mensagem)
        cursor.close()
    finally:
        release_faciais_conn(conn)
