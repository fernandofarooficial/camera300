from tracks import query_heimdall, fmt_timestamp, get_best_face, GENDER_MAP
from config import get_conn
from tracer import trace
from telegram import enviar_mensagem_telegram


def adequar_bases():
    trace("SISTEMA", "adequar_bases: iniciado — buscando registros sem image_path")
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM registros WHERE image_path IS NULL ORDER BY id LIMIT 200")
    linhas = cursor.fetchall()
    cursor.close()
    conn.close()
    total = len(linhas)
    trace("SISTEMA", f"adequar_bases: {total} registro(s) encontrado(s) para tratar")
    for e, lin in enumerate(linhas):
        track_id = lin["track_id"]
        idreg = lin["id"]
        trace("SISTEMA", f"adequar_bases: [{e + 1}/{total}] track_id={track_id} id={idreg}")
        atualizar_path_camera(track_id, idreg)
        admin_people(track_id)
    trace("SISTEMA", f"adequar_bases: concluído — {total} registro(s) tratado(s)")


def atualizar_path_camera(track_id, idreg):
    image_path, camera_id, face_det_score = get_best_face(track_id)
    if image_path is not None:
        conn = get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE registros SET image_path = %s, camera_id = %s, face_det_score = %s WHERE id = %s",
                (image_path, camera_id, face_det_score, idreg),
            )
            conn.commit()
            cursor.close()
        finally:
            conn.close()


def admin_people(track_id, data=None):
    faces = listar_matches_simples(track_id, data=data)
    id_unico = None
    for caretas in faces:
        track_careta = caretas.get('track_id')
        id_unico = obter_id_unico_legado(track_careta)
        if id_unico is not None:
            atualizar_id_unico_em_registro(id_unico, track_id)
            trace(track_id, f"admin_people: id_unico={id_unico} vinculado ao track_id={track_id}")
            telegram_cliente_chegou(track_id, id_unico)
            break
    if id_unico is None:
        trace(track_id, "admin_people: nenhum id_unico encontrado — criando nova pessoa")
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
                if score_raw is None or float(score_raw) < 0.73:
                    continue
            except (ValueError, TypeError):
                continue
            faces.append({
                "track_id": face.get("track_id", track_id),
                "image_path": face.get("image_path"),
                "timestamp": fmt_timestamp(face.get("timestamp")),
            })
    return faces


def obter_id_unico_legado(track_id):
    conn = get_conn()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT id_unico FROM registros WHERE track_id = %s ORDER BY id DESC LIMIT %s",
            (track_id, 1),
        )
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            return None
        return row.get('id_unico')
    finally:
        conn.close()


def atualizar_id_unico_em_registro(id_unico, track_id):
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE registros SET id_unico = %s WHERE track_id = %s",
            (id_unico, track_id),
        )
        conn.commit()
        cursor.close()
    finally:
        conn.close()


def criar_pessoa(track_id, data=None):
    genero = None
    idade = None
    if data:
        caras = (data.get("track") or {}).get("faces", [])
        for face in caras:
            if genero is None and face.get("gender") is not None:
                genero = GENDER_MAP.get(face.get("gender"))
            if idade is None and face.get("age") is not None:
                try:
                    idade = int(face.get("age"))
                except (ValueError, TypeError):
                    pass
            if genero is not None and idade is not None:
                break
        trace(track_id, f"criar_pessoa: genero={genero} idade={idade} extraídos do Heimdall")

    conn = get_conn()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "INSERT INTO pessoas (track_id_base, flag) VALUES (%s, %s)",
            (track_id, "C"),
        )
        conn.commit()
        cursor.execute(
            "SELECT id_unico FROM pessoas WHERE track_id_base = %s ORDER BY id_unico DESC LIMIT %s",
            (track_id, 1),
        )
        res = cursor.fetchone()
        if res is None:
            cursor.close()
            return
        iu = res.get("id_unico")
        nome = f"Anônimo{iu:03}"
        apelido = f"A{iu:03}"
        trace(track_id, f"criar_pessoa: nova pessoa criada → id_unico={iu} nome={nome}")
        cursor.execute(
            "UPDATE pessoas SET nome = %s, apelido = %s, genero = %s, idade = %s WHERE id_unico = %s",
            (nome, apelido, genero, idade, iu),
        )
        conn.commit()
        cursor.execute(
            "UPDATE registros SET id_unico = %s WHERE track_id = %s",
            (iu, track_id),
        )
        conn.commit()
        trace(track_id, f"criar_pessoa: registros atualizado com id_unico={iu}")
        cursor.close()
    finally:
        conn.close()


def telegram_cliente_chegou(track_id, id_unico):
    print(f"TCC(01): Entrei")
    conn = get_conn()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM pessoas WHERE id_unico = %s LIMIT 1", (id_unico,))
        pes = cursor.fetchone()
        print(f"TCC(02): pes={pes}")
        if pes is None:
            cursor.close()
            return
        if pes.get("flag", None) == "C":
            print(f"TCC(03): É cliente!")
            cursor.execute("""
                SELECT COUNT(*) AS qtd_hoje
                FROM registros
                WHERE id_unico = %s AND DATE(created_at) = CURDATE()
            """, (id_unico,))
            qtd_hoje = cursor.fetchone().get("qtd_hoje", 0)
            print(f"TCC(03b): qtd_hoje={qtd_hoje}")
            if qtd_hoje > 1:
                print(f"TCC(03c): Não é a primeira ocorrência do dia — abortando")
                cursor.close()
                return
            nome = pes.get("nome")
            nick = pes.get("apelido")
            nota = pes.get("notas")
            cursor.execute("SELECT COUNT(DISTINCT DATE(created_at)) AS qtd_visit FROM registros WHERE id_unico = %s", (id_unico,))
            vol_visit = cursor.fetchone()
            qtd_visit = vol_visit.get("qtd_visit")
            print(f"TCC(04): Select count")
            if qtd_visit >= 2:
                cursor.execute("""
                    SELECT created_at AS data_hora
                    FROM registros
                    WHERE id_unico = %s
                    ORDER BY created_at DESC
                    LIMIT 2;
                """, (id_unico,))
                visitas = cursor.fetchall()
                print(f"TCC(05): Select registros - duas visitas")
                ult_visit = visitas[0]["data_hora"].strftime("%d/%m/%Y %H:%M")
                ant_visit = visitas[1]["data_hora"].strftime("%d/%m/%Y %H:%M")
                msg1 = f"Cliente {nome}({nick}) chegou em {ult_visit}. "
                msg2 = f"Esta é a visita de número {qtd_visit} e visita anterior foi em {ant_visit}. "
                msg3 = f"Nota: {nota}" if nota is not None else "  "
                mensagem = msg1 + msg2 + msg3
            else:
                mensagem = f"Cliente {nome}({nick}) chegou. Esta é a sua primeira visita."
            print(f"TCC(06): Vou enviar mensagem")
            enviar_mensagem_telegram(mensagem)
        cursor.close()
    finally:
        conn.close()
