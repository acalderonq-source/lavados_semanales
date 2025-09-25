# app.py
import os, io, json, csv, uuid, hashlib, unicodedata, shutil
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ========= Utilidades básicas =========
def norm(s: Any) -> str:
    return unicodedata.normalize("NFD", str(s or "")).encode("ascii", "ignore").decode("ascii").lower().strip()

def iso_week_key(d: Optional[date] = None) -> str:
    d = d or date.today()
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def ensure_dirs():
    os.makedirs("store/evidence", exist_ok=True)
    os.makedirs("store/semanas", exist_ok=True)
    os.makedirs("data", exist_ok=True)

def load_json_file(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_json_file(path: str, data: Any):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def csv_bytes(rows: List[List[Any]]) -> bytes:
    buff = io.StringIO()
    writer = csv.writer(buff, quoting=csv.QUOTE_ALL)
    for r in rows:
        writer.writerow([("" if x is None else str(x)) for x in r])
    return buff.getvalue().encode("utf-8")

def xlsx_week_bytes(week: str, lav: List[Dict[str, Any]], nolav: List[Dict[str, Any]]) -> bytes:
    bio = io.BytesIO()
    df_lav = pd.DataFrame([{
        "week": week, "cedis": r["cedis"], "supervisor": r.get("supervisorNombre", ""),
        "segmento": r.get("segmento", ""), "unidadId": r.get("unidadId") or r.get("unidadLabel", ""),
        "timestamp": r.get("ts", ""), "created_by": r.get("created_by", "")
    } for r in lav])

    df_nolav = pd.DataFrame([{
        "week": week, "cedis": u["cedis"], "segmento": u["segmento"], "unidadId": u["id"]
    } for u in nolav])

    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        (df_lav if not df_lav.empty else pd.DataFrame(
            columns=["week","cedis","supervisor","segmento","unidadId","timestamp","created_by"])
        ).to_excel(writer, sheet_name="Lavadas", index=False)

        (df_nolav if not df_nolav.empty else pd.DataFrame(
            columns=["week","cedis","segmento","unidadId"])
        ).to_excel(writer, sheet_name="No_lavadas", index=False)

    bio.seek(0)
    return bio.getvalue()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def segment_from_negocio(neg: str) -> Tuple[str, str]:
    n = norm(neg)
    if "granel" in n:
        return "graneles", "Granel"
    if "cilindro" in n or "hino" in n:
        return "hinos", "Hino"
    return "otros", "Otro"

# ========= Config (catálogo/usuarios) =========
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
        {"id": "sup-miguel-gomez",   "nombre": "Miguel Gomez",   "cedis": "cartago",  "segmento":"hinos"},
        {"id": "sup-erick-valerin",  "nombre": "Erick Valerin",  "cedis": "cartago",  "segmento":"graneles"},
        # GUÁPILES
        {"id": "sup-enrique-herrera","nombre": "Enrique Herrera","cedis": "guapiles"},
        {"id": "sup-raul-retana",    "nombre": "Raul Retana",    "cedis": "guapiles","segmento":"hinos"},
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
        # TRANSPORTADORA
        {"id": "sup-ronny-garita",   "nombre": "Ronny Garita",   "cedis": "Transportadora"},
    ],
    "asignaciones": [
        # ejemplo: {"supervisorId":"sup-miguel-gomez", "unidadId":"C170135"},
    ],
}
USERS_PATH = "data/users.json"
STORE_PATH = "store/store.json"

def cedis_id_from_any(val: str) -> str:
    key = norm(val)
    for c in CONFIG["cedis"]:
        if norm(c["id"]) == key or norm(c["nombre"]) == key:
            return c["id"]
    return key

SOURCES: List[str] = [
    "data/unidades-hinos-cartago.json",
    "data/unidades-la-cruz.json",
    "data/unidades-alajuela.json",
    "data/unidades-todo.json",
    "data/unidades-transportadora.json",
]

def load_catalog() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for path in SOURCES:
        arr = load_json_file(path)
        if not isinstance(arr, list):
            continue
        for u in arr:
            id_ = str(u.get("id") or u.get("placa") or "").strip()
            if not id_:
                continue
            cedis = cedis_id_from_any(u.get("cedis", ""))
            if not cedis:
                continue
            segmento = u.get("segmento")
            tipo = u.get("tipo")
            if not segmento:
                segmento, tipo = segment_from_negocio(u.get("negocio", ""))
            if not tipo:
                tipo = "Hino" if segmento == "hinos" else "Granel" if segmento == "graneles" else "Otro"
            items.append({"id": id_, "cedis": cedis, "segmento": segmento, "tipo": tipo})
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for u in items:
        dedup[(u["id"], u["cedis"])] = u
    return list(dedup.values())

# ========= Usuarios / login =========
def load_users() -> Dict[str, Any]:
    data = load_json_file(USERS_PATH)
    if not isinstance(data, dict):
        return {"users": []}
    data.setdefault("users", [])
    return data

def save_users(data: Dict[str, Any]):
    save_json_file(USERS_PATH, data)

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def verify_password(user: Dict[str, Any], plain: str) -> bool:
    if "sha256" in user:
        return sha256_str(plain) == user["sha256"]
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

    btn = st.button("Entrar")
    if btn:
        u = next((u for u in users if norm(u.get("username")) == norm(username)), None)
        if not u or not verify_password(u, password):
            st.error("Usuario o contraseña incorrectos.")
            st.stop()
        auth = {
            "ok": True, "username": u["username"],
            "name": u.get("name") or u.get("nombre") or u["username"],
            "role": u.get("role", "supervisor"),
            "supervisorId": u.get("supervisor_id"),
        }
        st.session_state["auth"] = auth
        st.rerun()

    st.stop()

# ========= Persistencia local (JSON) =========
def load_store() -> Dict[str, Any]:
    data = load_json_file(STORE_PATH)
    if not isinstance(data, dict):
        return {"registros": {}}
    data.setdefault("registros", {})
    return data

def save_store(data: Dict[str, Any]):
    save_json_file(STORE_PATH, data)

def collect_all_photo_hashes_from_store(store: Dict[str, Any]) -> set:
    hashes = set()
    for lst in store.get("registros", {}).values():
        for r in lst:
            for h in (r.get("foto_hashes") or {}).values():
                if h:
                    hashes.add(h)
    return hashes

# ========= DB (opcional Neon) con fallback =========
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_URL = os.getenv("DATABASE_URL")
USE_DB = False
DB_ERR = None
engine = None

def db_init():
    with engine.begin() as conn:
        conn.execute(text("""
        create table if not exists lavados(
            id text primary key,
            week text not null,
            cedis text not null,
            supervisor_id text,
            supervisor_nombre text,
            unidad_id text not null,
            segmento text,
            ts timestamp,
            created_by text,
            fotos_json text,
            hashes_json text
        )
        """))
        conn.execute(text("""
        create unique index if not exists ux_lavado_week_cedis_unidad
        on lavados(week, cedis, unidad_id)
        """))

def db_photo_hashes_all() -> set:
    s = set()
    with engine.begin() as conn:
        for row in conn.execute(text("select hashes_json from lavados")):
            try:
                d = json.loads(row.hashes_json or "{}")
                s.update([v for v in d.values() if v])
            except Exception:
                pass
    return s

def db_get_lavados_week(week: str) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(text("""
            select * from lavados where week=:w
            order by ts desc
        """), {"w": week}).mappings().all()
    out = []
    for r in rows:
        out.append({
            "id": r["id"], "week": r["week"], "cedis": r["cedis"],
            "supervisorId": r["supervisor_id"], "supervisorNombre": r["supervisor_nombre"],
            "unidadId": r["unidad_id"], "unidadLabel": r["unidad_id"], "segmento": r["segmento"],
            "ts": (r["ts"].isoformat(timespec="seconds") if hasattr(r["ts"], "isoformat") else str(r["ts"])),
            "created_by": r["created_by"],
            "fotos": json.loads(r["fotos_json"] or "{}"),
            "foto_hashes": json.loads(r["hashes_json"] or "{}"),
        })
    return out

def db_save_lavado(record: Dict[str, Any]):
    with engine.begin() as conn:
        conn.execute(text("""
            delete from lavados where week=:w and cedis=:c and unidad_id=:u
        """), {"w": record["week"], "c": record["cedis"], "u": record["unidadId"]})
        conn.execute(text("""
            insert into lavados(id, week, cedis, supervisor_id, supervisor_nombre,
                                unidad_id, segmento, ts, created_by, fotos_json, hashes_json)
            values(:id, :week, :cedis, :sup_id, :sup_nom, :unidad, :seg, :ts, :created_by, :fotos, :hashes)
        """), {
            "id": record["id"], "week": record["week"], "cedis": record["cedis"],
            "sup_id": record.get("supervisorId"), "sup_nom": record.get("supervisorNombre"),
            "unidad": record["unidadId"], "seg": record.get("segmento"),
            "ts": datetime.fromisoformat(record["ts"]),
            "created_by": record.get("created_by",""),
            "fotos": json.dumps(record.get("fotos") or {}),
            "hashes": json.dumps(record.get("foto_hashes") or {}),
        })

def db_delete_lavado(rec_id: str):
    with engine.begin() as conn:
        conn.execute(text("delete from lavados where id=:i"), {"i": rec_id})

def db_delete_week(week: str):
    with engine.begin() as conn:
        conn.execute(text("delete from lavados where week=:w"), {"w": week})

# ======= Inicialización segura de DB (no bloqueante) =======
if DB_URL:
    try:
        engine = create_engine(DB_URL, future=True, pool_pre_ping=True, pool_recycle=300, pool_timeout=10)
        with engine.connect() as c:
            c.execute(text("select 1"))
        db_init()
        USE_DB = True
        # st.caption("DB (Neon) OK ✅")  # opcional mostrar
    except SQLAlchemyError as e:
        DB_ERR = str(e)
        USE_DB = False

# ========= App =========
st.set_page_config(page_title="Lavado semanal", layout="wide")
ensure_dirs()

auth = require_login()
CATALOGO = load_catalog()
cedis_labels = {c["id"]: c["nombre"] for c in CONFIG["cedis"]}
sup_by_id = {s["id"]: s for s in CONFIG["supervisores"]}

# Store en memoria (para fallback JSON)
STORE = load_store()
ALL_HASHES = db_photo_hashes_all() if USE_DB else collect_all_photo_hashes_from_store(STORE)

# Header
h1, h2 = st.columns([6, 1])
with h1:
    st.title("Lavado semanal de unidades")
    if DB_ERR:
        st.warning(f"No se pudo conectar a Neon, se usa modo local (JSON). Detalle: {DB_ERR}")
    st.caption(f"Usuario: **{auth['name']}** · Rol: **{auth['role']}**")
with h2:
    if st.button("Cerrar sesión"):
        st.session_state.pop("auth", None); st.rerun()

# Filtros superiores
cont = st.container()
with cont:
    cA, cB, cC, cD = st.columns([1.2, 1.5, 1.8, 1.8])

    with cA:
        fecha_sel = st.date_input("Semana (elige cualquier día)", value=date.today())
        WEEK = iso_week_key(fecha_sel)

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
            SUP = st.selectbox("Supervisor (para estadísticas)", options=[""]+[s["id"] for s in sup_list],
                               format_func=lambda x: (sup_map.get(x, {}) or {}).get("nombre", "— Elegir —"))

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
        if sup_seg:
            pool = [u for u in pool if u["segmento"] == sup_seg]
    if SEG != "all":
        pool = [u for u in pool if u["segmento"] == SEG]
    return pool

pool_cap = unidades_visibles()

# -------- Formulario de captura (solo supervisor) --------
st.subheader("Registrar lavado")
FOTO_SLOTS = [("frente","Frente"), ("atras","Atrás"), ("lado","Medio lado"), ("cabina","Cabina")]

def save_photo(file, subname: str, week: str, cedis: str, unidad_id: str) -> Optional[str]:
    if not file: return None
    ext = os.path.splitext(file.name or "")[1].lower() or ".jpg"
    base = os.path.join("store","evidence", week, norm(cedis).replace(" ","-"), str(unidad_id).replace("/","-"))
    os.makedirs(base, exist_ok=True)
    name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_{subname}{ext}"
    path = os.path.join(base, name)
    with open(path, "wb") as f:
        f.write(file.getbuffer())
    return path

if auth["role"] != "supervisor":
    st.info("El administrador no puede registrar ni modificar lavados. Solo consulta y exporta estadísticas.", icon="🔒")
else:
    with st.form("form_registro", clear_on_submit=False):
        unidad_ids = [u["id"] for u in pool_cap]
        unidad = st.selectbox("Unidad", options=[""]+unidad_ids, index=0)

        cols = st.columns(4)
        uploads: Dict[str, Any] = {}
        for (k, label), c in zip(FOTO_SLOTS, cols):
            with c:
                uploads[k] = st.file_uploader(f"Foto: {label}", type=["jpg","jpeg","png","webp"], key=f"u_{k}")

        submitted = st.form_submit_button("Guardar")
        if submitted:
            if not unidad:
                st.warning("Elegí la unidad.", icon="⚠️")
            elif any(uploads[k] is None for k,_ in FOTO_SLOTS):
                st.warning("Subí las 4 fotos: Frente, Atrás, Medio lado y Cabina.", icon="⚠️")
            else:
                # hashes locales y globales
                hashes_local: Dict[str,str] = {}
                duplicated_local = False
                for k,_ in FOTO_SLOTS:
                    h = sha256_bytes(uploads[k].getbuffer())
                    if h in hashes_local.values():
                        duplicated_local = True
                    hashes_local[k] = h
                if duplicated_local:
                    st.error("No podés subir la misma foto en dos posiciones distintas.", icon="🚫")
                    st.stop()
                ALL_HASHES = db_photo_hashes_all() if USE_DB else collect_all_photo_hashes_from_store(STORE)
                repeated_global = [k for k,h in hashes_local.items() if h in ALL_HASHES]
                if repeated_global:
                    st.error(f"Estas fotos ya se usaron antes: {', '.join(repeated_global)}.", icon="🚫")
                    st.stop()

                # Guardar fotos
                fotos_paths = {k: save_photo(uploads[k], k, WEEK, CEDIS, unidad) for k,_ in FOTO_SLOTS}

                u = next((x for x in CATALOGO if x["id"]==unidad and x["cedis"]==CEDIS), None)
                record = {
                    "id": uuid.uuid4().hex, "week": WEEK, "cedis": CEDIS,
                    "supervisorId": SUP, "supervisorNombre": (sup_by_id.get(SUP) or {}).get("nombre",""),
                    "unidadId": unidad, "unidadLabel": unidad, "segmento": (u or {}).get("segmento",""),
                    "fotos": fotos_paths, "foto_hashes": hashes_local,
                    "ts": datetime.now().isoformat(timespec="seconds"), "created_by": auth["username"],
                }

                if USE_DB:
                    db_save_lavado(record)
                else:
                    STORE.setdefault("registros", {})
                    lst = STORE["registros"].setdefault(WEEK, [])
                    lst = [r for r in lst if not (r["unidadId"]==unidad and r["cedis"]==CEDIS)]
                    lst.append(record)
                    STORE["registros"][WEEK] = lst
                    save_store(STORE)
                st.success("¡Guardado!")
                st.rerun()

# -------- Listado semana --------
st.subheader(f"Registros — {WEEK}")
reg_semana = db_get_lavados_week(WEEK) if USE_DB else STORE.get("registros", {}).get(WEEK, [])
if auth["role"] == "supervisor":
    reg_semana = [r for r in reg_semana if r["supervisorId"] == auth.get("supervisorId")]

if not reg_semana:
    st.write("Sin registros para esta semana.")
else:
    for rec in sorted(reg_semana, key=lambda x: x["ts"], reverse=True):
        cols = st.columns([1,1,0.9,1,2.2,0.9,0.6])
        cols[0].write(cedis_labels.get(rec["cedis"], rec["cedis"]))
        cols[1].write(rec.get("supervisorNombre","")); cols[2].write(rec.get("segmento",""))
        cols[3].write(rec.get("unidadLabel",""))
        gcols = cols[4].columns(4)
        for i,(k,_) in enumerate(FOTO_SLOTS):
            p = (rec.get("fotos") or {}).get(k)
            if p and os.path.exists(p):
                gcols[i].image(p, width="stretch")
            else:
                gcols[i].write("—")
        cols[5].write(rec["ts"])
        can_delete = auth["role"]=="supervisor" and rec["supervisorId"]==auth.get("supervisorId")
        if can_delete and cols[6].button("Eliminar", key=rec["id"]):
            if USE_DB:
                db_delete_lavado(rec["id"])
            else:
                STORE["registros"][WEEK] = [x for x in STORE["registros"][WEEK] if x["id"] != rec["id"]]
                save_store(STORE)
            st.rerun()
        if not can_delete: cols[6].write("—")

# -------- Resumen No lavadas --------
st.subheader(f"Unidades NO lavadas — {WEEK}")
CEDIS_RES = (sup_by_id.get(auth.get("supervisorId") or "", {}) or {}).get("cedis","") if auth["role"]=="supervisor" else CEDIS
st.write(f"CEDIS: **{cedis_labels.get(CEDIS_RES, CEDIS_RES)}**")

lavadas_set = {(r["unidadId"], r["cedis"]) for r in (db_get_lavados_week(WEEK) if USE_DB else STORE.get("registros", {}).get(WEEK, []))}
faltantes = [u for u in CATALOGO if (u["id"],u["cedis"]) not in lavadas_set and u["cedis"]==CEDIS_RES]

tabs = st.tabs([s["nombre"] for s in CONFIG["segmentos"]])
for i, seg in enumerate(CONFIG["segmentos"]):
    with tabs[i]:
        data = [u for u in faltantes if u["segmento"] == seg["id"]]
        st.write(f"Total: {len(data)}")
        if data:
            st.dataframe({"Unidad":[u["id"] for u in data], "Segmento":[u["segmento"] for u in data]}, width="stretch")
        else:
            st.success("¡Al día!")

# -------- Utilidades de semana (export carpeta + XLSX + eliminar) --------
def export_week_folders(week: str, catalog: List[Dict[str, Any]], registros: List[Dict[str,Any]], only_cedis: Optional[str]=None):
    base = os.path.join("store","semanas",week)
    lav_dir = os.path.join(base,"lavados"); nolav_dir = os.path.join(base,"no_lavados")
    os.makedirs(lav_dir, exist_ok=True); os.makedirs(nolav_dir, exist_ok=True)

    regs = registros[:] if not only_cedis else [r for r in registros if r["cedis"]==only_cedis]
    cat = catalog[:] if not only_cedis else [u for u in catalog if u["cedis"]==only_cedis]
    lavadas_set = {(r["cedis"], r["unidadId"]) for r in regs}

    for r in regs:
        cedis, unidad = r["cedis"], r["unidadId"]
        src_dir = os.path.join("store","evidence",week, norm(cedis).replace(" ","-"), str(unidad).replace("/","-"))
        dst_dir = os.path.join(lav_dir, cedis, unidad)
        os.makedirs(dst_dir, exist_ok=True)
        if os.path.isdir(src_dir):
            for name in os.listdir(src_dir):
                src = os.path.join(src_dir, name)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(dst_dir, name))
        save_json_file(os.path.join(dst_dir,"record.json"), r)

    for u in cat:
        key = (u["cedis"], u["id"])
        if key in lavadas_set: continue
        dst = os.path.join(nolav_dir, u["cedis"], u["id"])
        os.makedirs(dst, exist_ok=True)
        with open(os.path.join(dst,"README.txt"),"w",encoding="utf-8") as f:
            f.write(f"Unidad NO lavada en {week}\nCEDIS: {u['cedis']}\nSegmento: {u['segmento']}\n")

    rows = [["week","estado","cedis","segmento","unidadId","supervisor","timestamp"]]
    for r in regs:
        rows.append([week,"lavado",r["cedis"],r["segmento"],r["unidadId"],r.get("supervisorNombre",""),r["ts"]])
    for u in cat:
        if (u["cedis"],u["id"]) not in lavadas_set:
            rows.append([week,"no_lavado",u["cedis"],u["segmento"],u["id"],"", ""])
    with open(os.path.join(base,"resumen.csv"),"wb") as f:
        f.write(csv_bytes(rows))

    # guarda también XLSX en carpeta
    xlsx = xlsx_week_bytes(week, regs, [u for u in cat if (u["cedis"],u["id"]) not in lavadas_set])
    with open(os.path.join(base, "reporte.xlsx"), "wb") as f:
        f.write(xlsx)

# Botones utilitarios (panel común)
utilA, utilB, utilC = st.columns([1,1,1])
with utilA:
    if st.button("Crear carpeta de la semana"):
        registros = db_get_lavados_week(WEEK) if USE_DB else STORE.get("registros", {}).get(WEEK, [])
        export_week_folders(WEEK, CATALOGO, registros)
        st.success("Carpeta creada en store/semanas/")
with utilB:
    lav = db_get_lavados_week(WEEK) if USE_DB else STORE.get("registros", {}).get(WEEK, [])
    nolav = [u for u in CATALOGO if (u["id"],u["cedis"]) not in {(r["unidadId"], r["cedis"]) for r in lav}]
    xlsx_data = xlsx_week_bytes(WEEK, lav, nolav)
    st.download_button("Descargar XLSX (lavadas / no lavadas)", data=xlsx_data,
                       file_name=f"reporte-{WEEK}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with utilC:
    if auth["role"] == "admin":
        if st.button("Eliminar semana (DB/JSON + fotos)", type="primary"):
            # DB o JSON
            if USE_DB:
                db_delete_week(WEEK)
            else:
                STORE.get("registros", {}).pop(WEEK, None)
                save_store(STORE)
            # Borrar fotos y carpeta de semana
            shutil.rmtree(os.path.join("store","evidence", WEEK), ignore_errors=True)
            shutil.rmtree(os.path.join("store","semanas", WEEK), ignore_errors=True)
            st.success(f"Semana {WEEK} eliminada.")
            st.rerun()

# -------- Panel Admin: gestión de usuarios --------
def admin_user_manager():
    st.header("Gestión de usuarios")
    data = load_users(); users = data.get("users", [])
    sup_opts = {s["id"]: f'{s["nombre"]} · {cedis_labels.get(s["cedis"], s["cedis"])}' for s in CONFIG["supervisores"]}

    if users:
        st.subheader("Usuarios actuales")
        st.dataframe({
            "Usuario":[u.get("username","") for u in users],
            "Nombre":[u.get("name") or u.get("nombre","") for u in users],
            "Rol":[u.get("role","") for u in users],
            "Supervisor ID":[u.get("supervisor_id","") for u in users],
        }, width="stretch")
    else:
        st.info("No hay usuarios en data/users.json")

    st.markdown("---")
    st.subheader("Crear nuevo usuario")
    with st.form("crear_usuario", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            username = st.text_input("Usuario (sin espacios)").strip()
            name = st.text_input("Nombre completo").strip()
        with c2:
            role = st.selectbox("Rol", ["supervisor","admin"])
            password = st.text_input("Contraseña", type="password")

        sup_id = ""
        if role == "supervisor":
            sup_id = st.selectbox("Supervisor asignado", options=[""]+list(sup_opts.keys()),
                                  format_func=lambda x: sup_opts.get(x, "— Elegir —"))
        btn = st.form_submit_button("Crear usuario")
        if btn:
            if not username:
                st.error("Usuario obligatorio."); st.stop()
            if any(norm(u.get("username",""))==norm(username) for u in users):
                st.error("Ese usuario ya existe."); st.stop()
            if not password or len(password)<4:
                st.error("La contraseña debe tener al menos 4 caracteres."); st.stop()
            if role=="supervisor" and not sup_id:
                st.error("Elegí un supervisor asignado."); st.stop()
            new_user = {"username": username, "name": name or username, "role": role, "sha256": sha256_str(password)}
            if role=="supervisor": new_user["supervisor_id"] = sup_id
            users.append(new_user); save_users({"users": users})
            st.success(f"Usuario '{username}' creado."); st.rerun()

if auth["role"] == "admin":
    st.markdown("---")
    st.header(f"Panel del administrador — {WEEK}")
    admin_user_manager()
