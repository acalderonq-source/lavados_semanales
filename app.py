# app.py — Lavados Semanales (Streamlit)
# --------------------------------------
# - Login con roles (admin / supervisor)
# - Supervisores capturan lavados con 4 fotos obligatorias (frente, atrás, lado, cabina)
# - Bloqueo de fotos repetidas por hash SHA-256 (local/global)
# - Catálogos desde ./data/*.json (ver formatos abajo)
# - Export CSV/XLSX y export a carpetas por semana
# - Admin NO puede capturar ni borrar; solo ver, exportar y gestionar usuarios
# - Código protegido con boot-guard para mostrar errores en pantalla (evita "pantalla negra")
#
# Requisitos en requirements.txt (versiones estables en Render/Cloud):
# streamlit==1.38.0
# pandas==2.2.2
# xlsxwriter==3.2.9
# psycopg2-binary==2.9.10
# SQLAlchemy==2.0.43

from __future__ import annotations
import os, io, csv, json, uuid, hashlib, shutil, traceback
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ============== BOOT GUARD (muestra errores en pantalla) ==============
def boot_guard(fn):
    try:
        fn()
    except Exception as e:
        st.set_page_config(page_title="Error al iniciar", layout="wide")
        st.title("❌ La app falló al iniciar")
        st.error("Revisa el detalle del error y los logs del servidor.")
        st.exception(e)
        st.code("".join(traceback.format_exc()))
        st.stop()

# ========================= UTILIDADES BÁSICAS =========================
def norm(s: Any) -> str:
    """Minúsculas + sin acentos + trim."""
    import unicodedata
    return unicodedata.normalize("NFD", str(s or ""))\
        .encode("ascii", "ignore").decode("ascii")\
        .lower().strip()

def iso_week_key(d: Optional[date] = None) -> str:
    d = d or date.today()
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("store", exist_ok=True)
    os.makedirs("store/evidence", exist_ok=True)
    os.makedirs("store/semanas", exist_ok=True)

def load_json(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_json(path: str, data: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def csv_bytes(rows: List[List[Any]]) -> bytes:
    buff = io.StringIO()
    writer = csv.writer(buff, quoting=csv.QUOTE_ALL)
    for r in rows:
        writer.writerow([("" if x is None else str(x)) for x in r])
    return buff.getvalue().encode("utf-8")

def xlsx_week_bytes(week: str, lav: List[Dict[str, Any]], nolav: List[Dict[str, Any]]) -> bytes:
    bio = io.BytesIO()
    df_lav = pd.DataFrame([{
        "week": week,
        "cedis": r["cedis"],
        "supervisor": r.get("supervisorNombre", ""),
        "segmento": r.get("segmento", ""),
        "unidadId": r.get("unidadId") or r.get("unidadLabel", ""),
        "timestamp": r.get("ts", ""),
        "created_by": r.get("created_by", "")
    } for r in lav])
    df_nolav = pd.DataFrame([{
        "week": week, "cedis": u["cedis"], "segmento": u["segmento"], "unidadId": u["id"]
    } for u in nolav])

    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        (df_lav if not df_lav.empty else pd.DataFrame(
            columns=["week","cedis","supervisor","segmento","unidadId","timestamp","created_by"]
        )).to_excel(writer, sheet_name="Lavadas", index=False)

        (df_nolav if not df_nolav.empty else pd.DataFrame(
            columns=["week","cedis","segmento","unidadId"]
        )).to_excel(writer, sheet_name="No_lavadas", index=False)

    bio.seek(0)
    return bio.getvalue()

# ============================ CONFIG FIJA =============================
CONFIG: Dict[str, Any] = {
    "segmentos": [
        {"id": "hinos", "nombre": "Hinos"},
        {"id": "graneles", "nombre": "Graneles"},
        {"id": "otros", "nombre": "Otros"},
    ],
    "cedis": [
        {"id": "cartago", "nombre": "Cartago"},
        {"id": "alajuela", "nombre": "Alajuela"},
        {"id": "guapiles", "nombre": "Guápiles"},
        {"id": "Transportadora", "nombre": "Transportadora"},
        {"id": "San Carlos", "nombre": "San Carlos"},
        {"id": "Rio Claro", "nombre": "Rio Claro"},
        {"id": "Perez Zeledon", "nombre": "Perez Zeledon"},
        {"id": "Nicoya", "nombre": "Nicoya"},
        {"id": "La Cruz", "nombre": "La Cruz"},
    ],
    "supervisores": [
        # CARTAGO
        {"id": "sup-miguel-gomez",   "nombre": "Miguel Gomez",   "cedis": "cartago",  "segmento": "hinos"},
        {"id": "sup-erick-valerin",  "nombre": "Erick Valerin",  "cedis": "cartago",  "segmento": "graneles"},
        # GUÁPILES
        {"id": "sup-enrique-herrera","nombre": "Enrique Herrera","cedis": "guapiles"},
        {"id": "sup-raul-retana",    "nombre": "Raul Retana",    "cedis": "guapiles", "segmento": "hinos"},
        # PÉREZ ZELEDÓN
        {"id": "sup-adrian-veita",   "nombre": "Adrian Veita",   "cedis": "Perez Zeledon"},
        {"id": "sup-luis-solis",     "nombre": "Luis Solis",     "cedis": "Perez Zeledon"},
        # LA CRUZ
        {"id": "sup-daniel-salas",   "nombre": "Daniel Salas",   "cedis": "La Cruz"},
        {"id": "sup-roberto-chirino","nombre": "Roberto Chirino","cedis": "La Cruz"},
        # ALAJUELA
        {"id": "sup-cristian-bolanos","nombre":"Cristian Bolaños","cedis":"alajuela","segmento":"graneles"},
        {"id": "sup-roberto-vargas",  "nombre":"Roberto Vargas",  "cedis":"alajuela","segmento":"hinos"},
        # SAN CARLOS
        {"id": "sup-cristofer-carranza","nombre":"Cristofer Carranza","cedis":"San Carlos"},
        # RÍO CLARO
        {"id": "sup-victor-cordero", "nombre": "Victor Cordero", "cedis": "Rio Claro"},
        # NICOYA
        {"id": "sup-luis-rivas",     "nombre": "Luis Rivas",     "cedis": "Nicoya"},
    ],
    "asignaciones": [
        # ejemplo: {"supervisorId": "sup-miguel-gomez", "unidadId": "C170135"},
    ]
}

def cedis_id_from_any(val: str) -> str:
    key = norm(val)
    for c in CONFIG["cedis"]:
        if norm(c["id"]) == key or norm(c["nombre"]) == key:
            return c["id"]
    return key

def segment_from_negocio(neg: str) -> Tuple[str, str]:
    n = norm(neg)
    if "granel" in n: return "graneles", "Granel"
    if "cilindro" in n or "hino" in n: return "hinos", "Hino"
    return "otros", "Otro"

# ========================= CARGA DE CATÁLOGOS ========================
SOURCES: List[str] = [
    "data/unidades-hinos-cartago.json",
    "data/unidades-la-cruz.json",
    "data/unidades-alajuela.json",
    "data/unidades-todo.json",
    "data/unidades-transportadora.json",  # opcional
]

def load_catalog() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for path in SOURCES:
        arr = load_json(path)
        if not isinstance(arr, list):
            continue
        for u in arr:
            id_ = str(u.get("id") or u.get("placa") or "").strip()
            if not id_: continue
            cedis = cedis_id_from_any(u.get("cedis", ""))
            if not cedis: continue
            segmento = u.get("segmento"); tipo = u.get("tipo")
            if not segmento:
                segmento, tipo = segment_from_negocio(u.get("negocio", ""))
            if not tipo:
                tipo = "Hino" if segmento == "hinos" else "Granel" if segmento == "graneles" else "Otro"
            items.append({"id": id_, "cedis": cedis, "segmento": segmento, "tipo": tipo})
    # dedupe por (id, cedis)
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for u in items:
        dedup[(u["id"], u["cedis"])] = u
    return list(dedup.values())

# ============================= USUARIOS ==============================
USERS_PATH = "data/users.json"

def load_users() -> Dict[str, Any]:
    data = load_json(USERS_PATH)
    if not isinstance(data, dict):
        data = {"users": []}
    data.setdefault("users", [])
    # Si no hay admin, crear uno por defecto
    if not any(norm(u.get("username")) == "admin" for u in data["users"]):
        data["users"].append({"username": "admin", "name": "Administrador", "role": "admin", "password": "admin123"})
        save_users(data)
    return data

def save_users(data: Dict[str, Any]):
    save_json(USERS_PATH, data)

def verify_password(user: Dict[str, Any], plain: str) -> bool:
    if "sha256" in user:
        return sha256_bytes(plain.encode("utf-8")) == user["sha256"]
    if "password" in user:
        return user["password"] == plain
    return False

def require_login() -> Dict[str, Any]:
    if "auth" in st.session_state and st.session_state["auth"].get("ok"):
        return st.session_state["auth"]

    users = load_users().get("users", [])
    st.title("Lavado semanal de unidades")
    st.subheader("Iniciar sesión")

    c1, c2 = st.columns(2)
    with c1:
        username = st.text_input("Usuario")
    with c2:
        password = st.text_input("Contraseña", type="password")

    if st.button("Entrar"):
        u = next((u for u in users if norm(u.get("username")) == norm(username)), None)
        if not u or not verify_password(u, password):
            st.error("Usuario o contraseña incorrectos.")
            st.stop()
        st.session_state["auth"] = {
            "ok": True,
            "username": u["username"],
            "name": u.get("name") or u.get("nombre") or u["username"],
            "role": u.get("role", "supervisor"),
            "supervisorId": u.get("supervisor_id"),
        }
        st.rerun()
    st.stop()

def admin_user_manager(cedis_labels: Dict[str, str]):
    st.header("Gestión de usuarios")
    data = load_users()
    users = data.get("users", [])

    sup_opts = {s["id"]: f'{s["nombre"]} · {cedis_labels.get(s["cedis"], s["cedis"])}'
                for s in CONFIG["supervisores"]}

    if users:
        st.subheader("Usuarios actuales")
        st.dataframe({
            "Usuario": [u.get("username","") for u in users],
            "Nombre": [u.get("name") or u.get("nombre","") for u in users],
            "Rol":    [u.get("role","") for u in users],
            "Supervisor ID": [u.get("supervisor_id","") for u in users],
        }, width="stretch")
    else:
        st.info("No hay usuarios.")

    st.markdown("---")
    st.subheader("Crear nuevo usuario")
    with st.form("crear_usuario", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            username = st.text_input("Usuario (sin espacios)").strip()
            name = st.text_input("Nombre completo").strip()
        with col2:
            role = st.selectbox("Rol", ["supervisor", "admin"])
            password = st.text_input("Contraseña", type="password")

        sup_id = ""
        if role == "supervisor":
            sup_id = st.selectbox(
                "Supervisor asignado (obligatorio para rol supervisor)",
                options=[""] + list(sup_opts.keys()),
                format_func=lambda x: sup_opts.get(x, "— Elegir —")
            )
        btn = st.form_submit_button("Crear usuario")

        if btn:
            if not username:
                st.error("Usuario obligatorio."); st.stop()
            if any(norm(u.get("username","")) == norm(username) for u in users):
                st.error("Ese usuario ya existe."); st.stop()
            if not password or len(password) < 4:
                st.error("La contraseña debe tener al menos 4 caracteres."); st.stop()
            if role == "supervisor" and not sup_id:
                st.error("Elegí un supervisor para el usuario supervisor."); st.stop()

            new_user = {
                "username": username,
                "name": name or username,
                "role": role,
                "sha256": sha256_bytes(password.encode("utf-8")),
            }
            if role == "supervisor":
                new_user["supervisor_id"] = sup_id
            users.append(new_user)
            save_users({"users": users})
            st.success(f"Usuario '{username}' creado.")
            st.rerun()

# ========================== STORE (JSON LOCAL) ========================
STORE_PATH = "store/store.json"

def load_store() -> Dict[str, Any]:
    data = load_json(STORE_PATH)
    if not isinstance(data, dict):
        data = {"registros": {}}
    data.setdefault("registros", {})
    return data

def save_store(data: Dict[str, Any]):
    save_json(STORE_PATH, data)

def collect_all_photo_hashes(store: Dict[str, Any]) -> set:
    hashes = set()
    for lst in store.get("registros", {}).values():
        for r in lst:
            for h in (r.get("foto_hashes") or {}).values():
                if h: hashes.add(h)
    return hashes

def safe_slug(s: str) -> str:
    return norm(s).replace(" ", "-").replace("/", "-")

def save_photo(file, subname: str, week: str, cedis: str, unidad_id: str) -> Optional[str]:
    if not file: return None
    ext = os.path.splitext(file.name or "")[1].lower() or ".jpg"
    base = os.path.join("store", "evidence", week, safe_slug(cedis), safe_slug(str(unidad_id)))
    os.makedirs(base, exist_ok=True)
    name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_{subname}{ext}"
    path = os.path.join(base, name)
    with open(path, "wb") as f:
        f.write(file.getbuffer())
    return path

def delete_week(week: str, store: Dict[str, Any]):
    # borra registros + carpeta evidencia + carpeta semanas
    store["registros"].pop(week, None)
    save_store(store)
    try:
        shutil.rmtree(os.path.join("store", "evidence", week), ignore_errors=True)
        shutil.rmtree(os.path.join("store", "semanas", week), ignore_errors=True)
    except Exception:
        pass

def export_week_folders(week: str, catalog: List[Dict[str, Any]], store: Dict[str, Any], only_cedis: Optional[str] = None):
    base = os.path.join("store", "semanas", week)
    lav_dir = os.path.join(base, "lavados")
    nolav_dir = os.path.join(base, "no_lavados")
    os.makedirs(lav_dir, exist_ok=True); os.makedirs(nolav_dir, exist_ok=True)

    registros = store.get("registros", {}).get(week, [])[:]
    if only_cedis:
        registros = [r for r in registros if r["cedis"] == only_cedis]
        cat = [u for u in catalog if u["cedis"] == only_cedis]
    else:
        cat = catalog[:]

    lavadas_set = {(r["cedis"], r["unidadId"]) for r in registros}
    for r in registros:
        cedis = r["cedis"]; unidad = r["unidadId"]
        src_dir = os.path.join("store","evidence",week,safe_slug(cedis),safe_slug(unidad))
        dst_dir = os.path.join(lav_dir, cedis, unidad)
        os.makedirs(dst_dir, exist_ok=True)
        if os.path.isdir(src_dir):
            for name in os.listdir(src_dir):
                src = os.path.join(src_dir, name)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(dst_dir, name))
        save_json(os.path.join(dst_dir, "record.json"), r)

    for u in cat:
        if (u["cedis"], u["id"]) in lavadas_set: continue
        dst = os.path.join(nolav_dir, u["cedis"], u["id"])
        os.makedirs(dst, exist_ok=True)
        with open(os.path.join(dst,"README.txt"), "w", encoding="utf-8") as f:
            f.write(f"Unidad NO lavada en {week}\nCEDIS: {u['cedis']}\nSegmento: {u['segmento']}\nGenerado: {datetime.now().isoformat(timespec='seconds')}\n")

    rows = [["week","estado","cedis","segmento","unidadId","supervisor","timestamp"]]
    for r in registros:
        rows.append([week,"lavado",r["cedis"],r["segmento"],r["unidadId"],r.get("supervisorNombre",""),r["ts"]])
    for u in cat:
        if (u["cedis"], u["id"]) not in lavadas_set:
            rows.append([week,"no_lavado",u["cedis"],u["segmento"],u["id"],"",""])
    with open(os.path.join(base,"resumen.csv"),"wb") as f:
        f.write(csv_bytes(rows))

# ================================ APP ================================
def main():
    st.set_page_config(page_title="Lavado semanal", layout="wide")
    ensure_dirs()

    auth = require_login()     # obliga login
    CATALOGO = load_catalog()
    STORE = load_store()
    ALL_HASHES = collect_all_photo_hashes(STORE)

    cedis_labels = {c["id"]: c["nombre"] for c in CONFIG["cedis"]}
    sup_by_id = {s["id"]: s for s in CONFIG["supervisores"]}

    # Header + logout
    colH1, colH2 = st.columns([6,1])
    with colH1:
        st.title("Lavado semanal de unidades")
        st.caption(f"Usuario: **{auth['name']}** · Rol: **{auth['role']}**")
    with colH2:
        if st.button("Cerrar sesión"):
            st.session_state.pop("auth", None); st.rerun()

    # Filtros superiores
    cont = st.container()
    with cont:
        cA, cB, cC, cD = st.columns([1.1, 1.5, 1.6, 1.8])
        with cA:
            fecha_sel = st.date_input("Semana (elige cualquier día)", value=date.today())
            WEEK = iso_week_key(fecha_sel)

        # Rol: supervisor => cedis/sup fijos
        if auth["role"] == "supervisor":
            sup = sup_by_id.get(auth.get("supervisorId") or "", {})
            CEDIS = sup.get("cedis", "")
            SUP = sup.get("id", "")
            SUP_LABEL = sup.get("nombre", SUP)
            st.write(f"**CEDIS:** {cedis_labels.get(CEDIS, CEDIS)} · **Supervisor:** {SUP_LABEL}")
        else:
            with cB:
                cedis_options = [c["id"] for c in CONFIG["cedis"]]
                CEDIS = st.selectbox("Departamento (CEDIS)", options=cedis_options, index=0,
                                     format_func=lambda x: cedis_labels.get(x, x))
            with cC:
                sup_list = [s for s in CONFIG["supervisores"] if norm(s["cedis"]) == norm(CEDIS)]
                sup_map = {s["id"]: s for s in sup_list}
                SUP = st.selectbox(
                    "Supervisor (para estadísticas)",
                    options=[""] + [s["id"] for s in sup_list],
                    format_func=lambda x: (sup_map.get(x, {}) or {}).get("nombre", "— Elegir supervisor —"),
                )

        with cD:
            seg_ids = ["all"] + [s["id"] for s in CONFIG["segmentos"]]
            seg_labels = {"all":"Todos", **{s["id"]: s["nombre"] for s in CONFIG["segmentos"]}}
            SEG = st.radio("Segmento", options=seg_ids, format_func=lambda x: seg_labels[x], horizontal=True)

    sup_seg = (sup_by_id.get(SUP) or {}).get("segmento")

    def unidades_visibles() -> List[Dict[str, Any]]:
        if not SUP and auth["role"] == "supervisor":
            return []
        asignadas_ids = {a["unidadId"] for a in CONFIG["asignaciones"] if a["supervisorId"] == SUP}
        if asignadas_ids:
            pool = [u for u in CATALOGO if u["cedis"] == CEDIS and u["id"] in asignadas_ids]
        else:
            pool = [u for u in CATALOGO if u["cedis"] == CEDIS]
            if sup_seg:  # supervisor con segmento fijo
                pool = [u for u in pool if u["segmento"] == sup_seg]
        if SEG != "all":
            pool = [u for u in pool if u["segmento"] == SEG]
        return pool

    pool_cap = unidades_visibles()

    # -------- Formulario de captura (solo supervisor) --------
    st.subheader("Registrar lavado")
    FOTO_SLOTS = [("frente","Frente"),("atras","Atrás"),("lado","Medio lado"),("cabina","Cabina")]

    if auth["role"] != "supervisor":
        st.info("El administrador no puede registrar ni modificar lavados. Solo consulta y exporta estadísticas.", icon="🔒")
    else:
        with st.form("form_registro", clear_on_submit=False):
            unidad_ids = [u["id"] for u in pool_cap]
            unidad = st.selectbox("Unidad", options=[""] + unidad_ids, index=0)

            cols = st.columns(4)
            uploads: Dict[str, Any] = {}
            for (key, label), c in zip(FOTO_SLOTS, cols):
                with c:
                    uploads[key] = st.file_uploader(
                        f"Foto: {label}", type=["jpg","jpeg","png","webp"], key=f"u_{key}"
                    )

            submitted = st.form_submit_button("Guardar")
            if submitted:
                if not unidad:
                    st.warning("Elegí la unidad.", icon="⚠️")
                elif any(uploads[k] is None for k, _ in FOTO_SLOTS):
                    st.warning("Subí las 4 fotos: Frente, Atrás, Medio lado y Cabina.", icon="⚠️")
                else:
                    # Duplicidad local (en el mismo form)
                    hashes_local: Dict[str, str] = {}
                    dup_local = False
                    for k,_ in FOTO_SLOTS:
                        h = sha256_bytes(uploads[k].getbuffer())
                        if h in hashes_local.values(): dup_local = True
                        hashes_local[k] = h
                    if dup_local:
                        st.error("No podés subir la misma foto en dos posiciones distintas.", icon="🚫"); st.stop()

                    # Duplicidad global
                    ALL_HASHES = collect_all_photo_hashes(STORE)
                    repetidas = [k for k,h in hashes_local.items() if h in ALL_HASHES]
                    if repetidas:
                        st.error(f"Estas fotos ya se usaron antes: {', '.join(repetidas)}.", icon="🚫"); st.stop()

                    # Guardar fotos
                    fotos_paths = {k: save_photo(uploads[k], k, WEEK, CEDIS, unidad) for k,_ in FOTO_SLOTS}

                    u = next((x for x in CATALOGO if x["id"] == unidad and x["cedis"] == CEDIS), None)
                    record = {
                        "id": uuid.uuid4().hex,
                        "week": WEEK,
                        "cedis": CEDIS,
                        "supervisorId": SUP,
                        "supervisorNombre": (sup_by_id.get(SUP) or {}).get("nombre",""),
                        "unidadId": unidad, "unidadLabel": unidad,
                        "segmento": (u or {}).get("segmento",""),
                        "fotos": fotos_paths, "foto_hashes": hashes_local,
                        "ts": datetime.now().isoformat(timespec="seconds"),
                        "created_by": auth["username"],
                    }
                    STORE.setdefault("registros", {})
                    lst = STORE["registros"].setdefault(WEEK, [])
                    lst = [r for r in lst if not (r["unidadId"] == unidad and r["cedis"] == CEDIS)]
                    lst.append(record)
                    STORE["registros"][WEEK] = lst
                    save_store(STORE)
                    st.success("¡Guardado!")

    # -------- Tabla de registros de la semana --------
    st.subheader(f"Registros — {iso_week_key(fecha_sel)}")
    reg_semana = STORE.get("registros", {}).get(iso_week_key(fecha_sel), [])
    if auth["role"] == "supervisor":
        reg_semana = [r for r in reg_semana if r["supervisorId"] == auth.get("supervisorId")]

    if not reg_semana:
        st.write("Sin registros para esta semana.")
    else:
        for r in sorted(reg_semana, key=lambda x: x["ts"], reverse=True):
            cols = st.columns([1,1,0.8,1,2.2,0.9,0.6])
            cols[0].write(cedis_labels.get(r["cedis"], r["cedis"]))
            cols[1].write(r.get("supervisorNombre",""))
            cols[2].write(r.get("segmento",""))
            cols[3].write(r.get("unidadLabel",""))
            gcols = cols[4].columns(4)
            for i,(k,_) in enumerate(FOTO_SLOTS):
                p = (r.get("fotos") or {}).get(k)
                gcols[i].image(p, width="stretch") if p and os.path.exists(p) else gcols[i].write("—")
            cols[5].write(r["ts"])
            can_delete = auth["role"] == "supervisor" and r["supervisorId"] == auth.get("supervisorId")
            if can_delete and cols[6].button("Eliminar", key=r["id"]):
                STORE["registros"][iso_week_key(fecha_sel)] = [x for x in STORE["registros"][iso_week_key(fecha_sel)] if x["id"] != r["id"]]
                save_store(STORE)
                export_week_folders(iso_week_key(fecha_sel), CATALOGO, STORE)
                st.rerun()
            if not can_delete:
                cols[6].write("—")

    # -------- Resumen: No lavadas (según CEDIS) --------
    st.subheader(f"Unidades NO lavadas — {iso_week_key(fecha_sel)}")
    if auth["role"] == "supervisor":
        CEDIS_RES = (sup_by_id.get(auth.get("supervisorId") or "", {}) or {}).get("cedis","")
    else:
        CEDIS_RES = CEDIS

    lavadas_set = {(r["unidadId"], r["cedis"]) for r in STORE.get("registros", {}).get(iso_week_key(fecha_sel), [])}
    faltantes = [u for u in CATALOGO if (u["id"], u["cedis"]) not in lavadas_set and u["cedis"] == CEDIS_RES]

    tabs = st.tabs([s["nombre"] for s in CONFIG["segmentos"]])
    for i, seg in enumerate(CONFIG["segmentos"]):
        with tabs[i]:
            data = [u for u in faltantes if u["segmento"] == seg["id"]]
            st.write(f"Total: {len(data)}")
            if data:
                st.dataframe({
                    "Unidad": [u["id"] for u in data],
                    "Segmento": [u["segmento"] for u in data]
                }, width="stretch")
            else:
                st.success("¡Al día!")

    # -------- Panel del administrador --------
    if auth["role"] == "admin":
        st.markdown("---")
        st.header(f"Panel del administrador — {iso_week_key(fecha_sel)}")

        c1, c2, c3, c4 = st.columns([1,1,1,2])
        with c1:
            admin_cedis = st.selectbox(
                "CEDIS",
                options=["all"] + [c["id"] for c in CONFIG["cedis"]],
                format_func=lambda x: "Todos" if x=="all" else cedis_labels.get(x, x),
            )
        with c2:
            admin_seg = st.selectbox(
                "Segmento",
                options=["all"] + [s["id"] for s in CONFIG["segmentos"]],
                format_func=lambda x: "Todos" if x=="all" else next(s["nombre"] for s in CONFIG["segmentos"] if s["id"]==x),
            )
        with c3:
            sup_all = CONFIG["supervisores"] if admin_cedis=="all" else [s for s in CONFIG["supervisores"] if norm(s["cedis"])==norm(admin_cedis)]
            sup_map_all = {s["id"]: s for s in sup_all}
            admin_sup = st.selectbox(
                "Supervisor",
                options=["all"] + [s["id"] for s in sup_all],
                format_func=lambda x: "Todos" if x=="all" else sup_map_all.get(x,{}).get("nombre",""),
            )
        with c4:
            admin_q = st.text_input("Buscar (unidad o supervisor)")

        pool = CATALOGO[:]
        if admin_cedis!="all": pool = [u for u in pool if u["cedis"] == admin_cedis]
        if admin_seg!="all":   pool = [u for u in pool if u["segmento"] == admin_seg]
        if admin_sup!="all":
            ids_asig = {a["unidadId"] for a in CONFIG["asignaciones"] if a["supervisorId"] == admin_sup}
            if ids_asig: pool = [u for u in pool if u["id"] in ids_asig]
        if admin_q.strip():
            q = norm(admin_q)
            pool = [u for u in pool if q in norm(u["id"]) or q in norm(cedis_labels.get(u["cedis"], u["cedis"]))]

        lav = STORE.get("registros", {}).get(iso_week_key(fecha_sel), [])[:]
        if admin_cedis!="all": lav = [r for r in lav if r["cedis"] == admin_cedis]
        if admin_seg!="all":   lav = [r for r in lav if r["segmento"] == admin_seg]
        if admin_sup!="all":   lav = [r for r in lav if r["supervisorId"] == admin_sup]
        if admin_q.strip():
            q = norm(admin_q)
            lav = [r for r in lav if q in norm(r["unidadLabel"]) or q in norm(r["supervisorNombre"])]

        nolav = [u for u in pool if (u["id"], u["cedis"]) not in {(r["unidadId"], r["cedis"]) for r in lav}]

        # Descargar XLSX combinado
        xlsx_data = xlsx_week_bytes(iso_week_key(fecha_sel), lav, nolav)
        st.download_button(
            "Descargar XLSX (lavadas / no lavadas)",
            data=xlsx_data,
            file_name=f"reporte-{iso_week_key(fecha_sel)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        cA, cB = st.columns(2)
        with cA:
            st.subheader("Lavadas (filtros)")
            st.write(f"Total: {len(lav)}")
            if lav:
                st.dataframe({
                    "CEDIS": [cedis_labels.get(r["cedis"], r["cedis"]) for r in lav],
                    "Supervisor": [r["supervisorNombre"] for r in lav],
                    "Segmento": [r["segmento"] for r in lav],
                    "Unidad": [r["unidadLabel"] for r in lav],
                    "Fecha": [r["ts"] for r in lav],
                    "Capturado por": [r.get("created_by","") for r in lav],
                }, width="stretch")
            csv_lav = csv_bytes(
                [["week","cedis","supervisor","segmento","unidadId","timestamp","created_by"], *[
                    [iso_week_key(fecha_sel), r["cedis"], r["supervisorNombre"], r["segmento"],
                     r["unidadLabel"], r["ts"], r.get("created_by","")] for r in lav
                ]]
            )
            st.download_button("Exportar LAVADAS (CSV)", data=csv_lav, file_name=f"lavadas-{iso_week_key(fecha_sel)}.csv", mime="text/csv")

        with cB:
            st.subheader("No lavadas (filtros)")
            st.write(f"Total: {len(nolav)}")
            if nolav:
                st.dataframe({
                    "CEDIS": [cedis_labels.get(u["cedis"], u["cedis"]) for u in nolav],
                    "Segmento": [u["segmento"] for u in nolav],
                    "Unidad": [u["id"] for u in nolav],
                }, width="stretch")
            csv_nolav = csv_bytes(
                [["week","cedis","segmento","unidadId"], *[
                    [iso_week_key(fecha_sel), u["cedis"], u["segmento"], u["id"]] for u in nolav
                ]]
            )
            st.download_button("Exportar NO LAVADAS (CSV)", data=csv_nolav, file_name=f"no-lavadas-{iso_week_key(fecha_sel)}.csv", mime="text/csv")

        st.markdown("---")
        # Export a carpetas + eliminar semana
        cX, cY = st.columns([1,1])
        with cX:
            if st.button("Generar carpetas de la semana (lavados / no_lavados)"):
                export_week_folders(iso_week_key(fecha_sel), CATALOGO, STORE,
                                    only_cedis=None if admin_cedis=="all" else admin_cedis)
                st.success("Carpetas listas en store/semanas/")
        with cY:
            if st.button(f"Eliminar TODO la semana {iso_week_key(fecha_sel)}", type="primary"):
                delete_week(iso_week_key(fecha_sel), STORE)
                st.success("Semana eliminada."); st.rerun()

        st.markdown("---")
        admin_user_manager(cedis_labels)

# ---- run ----
if __name__ == "__main__":
    boot_guard(main)
