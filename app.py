# app.py — Lavados Semanales (Streamlit)
# --------------------------------------
# - Login con roles (admin / supervisor) contra BD MySQL (Railway)
# - Supervisores capturan lavados con 4 fotos (frente, atrás, lado, cabina)
# - Bloqueo de fotos repetidas por hash SHA-256 (global, consultando BD)
# - Catálogos desde ./data/*.json
# - Export CSV/XLSX y export a carpetas por semana
# - Admin NO captura ni borra; solo ver/exportar/gestionar usuarios
# - Reportes y gráficos (KPIs + barras por CEDIS / supervisor)
# - Boot-guard: muestra errores en pantalla

from __future__ import annotations

import os, io, csv, json, uuid, hashlib, shutil, traceback
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# Capa de datos (asegúrate que db.py exista y exporte estas funciones)
from db import (
    init_db, healthcheck,
    upsert_user, get_user, list_users,
    save_lavado, get_lavados_week, delete_lavado, photo_hashes_all
)

# ======================= Branding / Estilos =======================

LOGO_URL = "https://tse1.mm.bing.net/th/id/OIP.QBCt9-dF3e4xLmEw_WVPmQHaCW?rs=1&pid=ImgDetMain&o=7&rm=3"

def inject_css():
    st.markdown("""
    <style>
      .main { padding-top: 0.25rem; }
      div.block-container { max-width: 1200px; }

      .app-header {
        display:flex; align-items:center; gap:14px;
        background:#fff; border:1px solid #eaeaea;
        border-radius:16px; padding:12px 16px; 
        box-shadow:0 8px 20px rgba(0,0,0,.04);
        margin-bottom:12px;
      }
      .app-header .logo { height:48px; border-radius:8px; }
      .app-header .title { font-weight:700; font-size:20px; letter-spacing:.2px; color:#0f172a; }
      .app-header .subtitle { font-size:12px; color:#64748b; margin-top:-2px; }

      .stButton > button {
        border-radius:12px; padding:10px 16px; font-weight:600;
        border:1px solid #0ea5e9; background:#0ea5e9; color:#fff;
        box-shadow:0 2px 0 rgba(14,165,233,.15);
      }
      .stButton > button:hover { filter: brightness(.97); }

      div[role="radiogroup"] label {
        border:1px solid #e5e7eb; border-radius:999px; padding:8px 14px; margin-right:8px;
      }

      .stDataFrame tbody tr:nth-child(odd){ background:#fafafa; }
      .stDataFrame thead tr th { background:#f6f8fa !important; }

      .stFileUploader { border-radius:12px; }
      #MainMenu, footer {visibility:hidden;}
    </style>
    """, unsafe_allow_html=True)

# ========================== Boot Guard ===========================

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

# ========================= Utilidades base =======================

BASE_DIR     = os.getenv("DATA_DIR", "store")   # raíz de datos (archivos)
EVIDENCE_DIR = os.path.join(BASE_DIR, "evidence")
WEEKS_DIR    = os.path.join(BASE_DIR, "semanas")

def norm(s: Any) -> str:
    import unicodedata
    return unicodedata.normalize("NFD", str(s or ""))\
        .encode("ascii", "ignore").decode("ascii")\
        .lower().strip()

def iso_week_key(d: Optional[date] = None) -> str:
    d = d or date.today()
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def safe_slug(s: str) -> str:
    return norm(s).replace(" ", "-").replace("/", "-")

def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    os.makedirs(WEEKS_DIR, exist_ok=True)

def load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_json(path: str, data: Any):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def csv_bytes(rows: List[List[Any]]) -> bytes:
    buff = io.StringIO(newline="")
    writer = csv.writer(buff, quoting=csv.QUOTE_ALL)
    for r in rows:
        writer.writerow(["" if x is None else str(x) for x in r])
    return buff.getvalue().encode("utf-8")

def xlsx_week_bytes(week: str, lav: List[Dict[str, Any]], nolav: List[Dict[str, Any]]) -> bytes:
    bio = io.BytesIO()
    df_lav = pd.DataFrame([{
        "week": week,
        "cedis": r.get("cedis", ""),
        "supervisor": r.get("supervisorNombre", ""),
        "segmento": r.get("segmento", ""),
        "unidadId": r.get("unidadId") or r.get("unidadLabel", ""),
        "timestamp": r.get("ts", ""),
        "created_by": r.get("created_by", "")
    } for r in (lav or [])])

    df_nolav = pd.DataFrame([{
        "week": week,
        "cedis": u.get("cedis", ""),
        "segmento": u.get("segmento", ""),
        "unidadId": u.get("id", "")
    } for u in (nolav or [])])

    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        (df_lav if not df_lav.empty else pd.DataFrame(
            columns=["week","cedis","supervisor","segmento","unidadId","timestamp","created_by"]
        )).to_excel(writer, sheet_name="Lavadas", index=False)

        (df_nolav if not df_nolav.empty else pd.DataFrame(
            columns=["week","cedis","segmento","unidadId"]
        )).to_excel(writer, sheet_name="No_lavadas", index=False)

    bio.seek(0)
    return bio.getvalue()

# ============================ Config fija =========================

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
        {"id": "Tecnicos", "nombre": "Tecnicos"},
        {"id": "San Carlos", "nombre": "San Carlos"},
        {"id": "Rio Claro", "nombre": "Rio Claro"},
        {"id": "Perez Zeledon", "nombre": "Perez Zeledon"},
        {"id": "Nicoya", "nombre": "Nicoya"},
        {"id": "La Cruz", "nombre": "La Cruz"},
    ],
    "supervisores": [
        {"id": "sup-lorem-salazar", "nombre": "Loren Salazar", "cedis": "Tecnicos"},
        {"id": "sup-ronny-garita", "nombre": "Ronny Garita", "cedis": "Transportadora"},
        {"id": "sup-miguel-gomez", "nombre": "Miguel Gomez", "cedis": "cartago",  "segmento": "hinos"},
        {"id": "sup-erick-valerin","nombre": "Erick Valerin","cedis": "cartago",  "segmento": "graneles"},
        {"id": "sup-enrique-herrera","nombre": "Enrique Herrera","cedis": "guapiles"},
        {"id": "sup-raul-retana",   "nombre": "Raul Retana",   "cedis": "guapiles", "segmento": "hinos"},
        {"id": "sup-adrian-veita",  "nombre": "Adrian Veita",  "cedis": "Perez Zeledon", "segmento": "graneles"},
        {"id": "sup-luis-solis",    "nombre": "Luis Solis",    "cedis": "Perez Zeledon", "segmento": "hinos"},
        {"id": "sup-daniel-salas",  "nombre": "Daniel Salas",  "cedis": "La Cruz"},
        {"id": "sup-roberto-chirino","nombre": "Roberto Chirino","cedis": "La Cruz"},
        {"id": "sup-cristian-bolanos","nombre":"Cristian Bolaños","cedis":"alajuela","segmento":"graneles"},
        {"id": "sup-roberto-vargas",  "nombre":"Roberto Vargas",  "cedis":"alajuela","segmento":"hinos"},
        {"id": "sup-cristofer-carranza","nombre":"Cristofer Carranza","cedis":"San Carlos"},
        {"id": "sup-victor-cordero", "nombre":"Victor Cordero", "cedis": "Rio Claro"},
        {"id": "sup-luis-rivas",     "nombre":"Luis Rivas",     "cedis": "Nicoya"},
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

# ======================== Catálogos (data/*.json) ====================

SOURCES: List[str] = [
    "data/unidades-hinos-cartago.json",
    "data/unidades-la-cruz.json",
    "data/unidades-alajuela.json",
    "data/unidades-todo.json",
    "data/unidades-transportadora.json",
    "data/unidades-tecnicos.json",
]

def load_catalog() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for path in SOURCES:
        arr = load_json(path)
        if not isinstance(arr, list):
            continue
        for u in arr:
            id_ = str(u.get("id") or u.get("placa") or "").strip()
            if not id_:
                continue
            cedis = cedis_id_from_any(u.get("cedis", ""))
            if not cedis:
                continue
            segmento = u.get("segmento"); tipo = u.get("tipo")
            if not segmento:
                segmento, tipo = segment_from_negocio(u.get("negocio", ""))
            if not tipo:
                tipo = "Hino" if segmento == "hinos" else "Granel" if segmento == "graneles" else "Otro"
            items.append({"id": id_, "cedis": cedis, "segmento": segmento, "tipo": tipo})
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for u in items:
        dedup[(u["id"], u["cedis"])] = u
    return list(dedup.values())

# ============================== Usuarios ============================

def require_login() -> Dict[str, Any]:
    if "auth" in st.session_state and st.session_state["auth"].get("ok"):
        return st.session_state["auth"]

    st.title("Lavado semanal de unidades")
    st.subheader("Iniciar sesión")

    c1, c2 = st.columns(2)
    with c1:
        username = st.text_input("Usuario").strip()
    with c2:
        password = st.text_input("Contraseña", type="password")

    if st.button("Entrar"):
        u = get_user(username)  # DB
        if not u:
            st.error("Usuario o contraseña incorrectos."); st.stop()
        pwd_ok = (u.get("sha256") == hashlib.sha256(password.encode("utf-8")).hexdigest())
        if not pwd_ok:
            st.error("Usuario o contraseña incorrectos."); st.stop()

        st.session_state["auth"] = {
            "ok": True,
            "username": u["username"],
            "name": u.get("name") or u["username"],
            "role": u.get("role", "supervisor"),
            "supervisorId": u.get("supervisor_id"),
        }
        st.rerun()

    st.stop()

def admin_user_manager(cedis_labels: Dict[str, str]):
    st.header("Gestión de usuarios")
    users = list_users()  # DB

    sup_opts = {s["id"]: f'{s["nombre"]} · {cedis_labels.get(s["cedis"], s["cedis"])}'
                for s in CONFIG["supervisores"]}

    if users:
        st.subheader("Usuarios actuales")
        st.dataframe({
            "Usuario": [u.get("username","") for u in users],
            "Nombre":  [u.get("name","") for u in users],
            "Rol":     [u.get("role","") for u in users],
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
            if not password or len(password) < 4:
                st.error("La contraseña debe tener al menos 4 caracteres."); st.stop()
            if role == "supervisor" and not sup_id:
                st.error("Elegí un supervisor para el usuario supervisor."); st.stop()

            new_user = {
                "username": username,
                "name": name or username,
                "role": role,
                "sha256": hashlib.sha256(password.encode("utf-8")).hexdigest(),
                "supervisor_id": sup_id if role == "supervisor" else None,
            }
            upsert_user(new_user)  # DB
            st.success(f"Usuario '{username}' creado.")
            st.rerun()

# ============================= Fotos / Export ===========================

def save_photo(file, subname: str, week: str, cedis: str, unidad_id: str) -> Optional[str]:
    if not file:
        return None
    ext = os.path.splitext(file.name or "")[1].lower() or ".jpg"
    base = os.path.join(EVIDENCE_DIR, week, safe_slug(cedis), safe_slug(str(unidad_id)))
    os.makedirs(base, exist_ok=True)
    name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_{subname}{ext}"
    path = os.path.join(base, name)
    with open(path, "wb") as f:
        f.write(file.getbuffer())
    return path

def export_week_folders(week: str, catalog: List[Dict[str, Any]], registros_semana: List[Dict[str, Any]], only_cedis: Optional[str] = None):
    base = os.path.join(WEEKS_DIR, week)
    lav_dir = os.path.join(base, "lavados")
    nolav_dir = os.path.join(base, "no_lavados")
    os.makedirs(lav_dir, exist_ok=True)
    os.makedirs(nolav_dir, exist_ok=True)

    registros = registros_semana[:]
    if only_cedis:
        registros = [r for r in registros if r["cedis"] == only_cedis]
        cat = [u for u in catalog if u["cedis"] == only_cedis]
    else:
        cat = catalog[:]

    lavadas_set = {(r["cedis"], r["unidadId"]) for r in registros}

    for r in registros:
        cedis = r["cedis"]; unidad = r["unidadId"]
        src_dir = os.path.join(EVIDENCE_DIR, week, safe_slug(cedis), safe_slug(unidad))
        dst_dir = os.path.join(lav_dir, cedis, unidad)
        os.makedirs(dst_dir, exist_ok=True)
        if os.path.isdir(src_dir):
            for name in os.listdir(src_dir):
                src = os.path.join(src_dir, name)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(dst_dir, name))
        save_json(os.path.join(dst_dir, "record.json"), r)

    for u in cat:
        if (u["cedis"], u["id"]) in lavadas_set:
            continue
        dst = os.path.join(nolav_dir, u["cedis"], u["id"])
        os.makedirs(dst, exist_ok=True)
        with open(os.path.join(dst, "README.txt"), "w", encoding="utf-8") as f:
            f.write(
                f"Unidad NO lavada en {week}\n"
                f"CEDIS: {u['cedis']}\n"
                f"Segmento: {u['segmento']}\n"
                f"Generado: {datetime.now().isoformat(timespec='seconds')}\n"
            )

    rows = [["week","estado","cedis","segmento","unidadId","supervisor","timestamp"]]
    for r in registros:
        rows.append([week, "lavado", r["cedis"], r["segmento"], r["unidadId"], r.get("supervisorNombre",""), r["ts"]])
    for u in cat:
        if (u["cedis"], u["id"]) not in lavadas_set:
            rows.append([week, "no_lavado", u["cedis"], u["segmento"], u["id"], "", ""])

    with open(os.path.join(base, "resumen.csv"), "wb") as f:
        f.write(csv_bytes(rows))

def delete_week_everywhere(week: str, registros_semana: List[Dict[str, Any]]):
    # borra en BD todos los registros de esa semana + limpia carpetas locales
    for r in registros_semana:
        try:
            delete_lavado(r["id"])
        except Exception:
            pass
    shutil.rmtree(os.path.join(EVIDENCE_DIR, week), ignore_errors=True)
    shutil.rmtree(os.path.join(WEEKS_DIR, week), ignore_errors=True)

# ============================ Reportes & Gráficos ========================

def kpis_y_graficos(
    CATALOGO: List[Dict[str, Any]],
    reg_semana: List[Dict[str, Any]],
    sup_by_id: Dict[str, Dict[str, Any]],
    cedis_labels: Dict[str, str],
    week_key: str,
    cedis_filtro: Optional[str] = None
):
    st.subheader("Reportes y Gráficos")

    # Filtrar por CEDIS si aplica
    if cedis_filtro:
        catalog_fil = [u for u in CATALOGO if u["cedis"] == cedis_filtro]
        regs_fil    = [r for r in reg_semana if r["cedis"] == cedis_filtro]
    else:
        catalog_fil = CATALOGO[:]
        regs_fil    = reg_semana[:]

    total_unidades = len({(u["id"], u["cedis"]) for u in catalog_fil})
    lavadas_set = {(r["unidadId"], r["cedis"]) for r in regs_fil}
    total_lavadas = len(lavadas_set)
    total_no_lav = total_unidades - total_lavadas
    pct = (total_lavadas / total_unidades * 100.0) if total_unidades else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unidades totales", total_unidades)
    c2.metric("Lavadas", total_lavadas)
    c3.metric("No lavadas", total_no_lav)
    c4.metric("Cumplimiento (%)", f"{pct:.1f}%")

    # --- Barras por CEDIS ---
    st.markdown("**Lavadas por CEDIS**")
    df_cedis = pd.DataFrame([{"cedis": r["cedis"]} for r in regs_fil]).value_counts().reset_index(name="lavadas")
    if not df_cedis.empty:
        df_cedis["CEDIS"] = df_cedis["cedis"].map(lambda x: cedis_labels.get(x, x))
        df_cedis = df_cedis.sort_values("lavadas", ascending=False)
        st.bar_chart(data=df_cedis.set_index("CEDIS")["lavadas"], use_container_width=True)
    else:
        st.info("Sin lavados registrados para el filtro seleccionado.")

    # --- Barras por Supervisor (lavadas) ---
    st.markdown("**Lavadas por Supervisor**")
    df_sup = pd.DataFrame([{"sup": r.get("supervisorNombre","(sin supervisor)")} for r in regs_fil]) \
                .value_counts().reset_index(name="lavadas")
    if not df_sup.empty:
        df_sup = df_sup.rename(columns={"sup":"Supervisor"}).sort_values("lavadas", ascending=False)
        st.bar_chart(data=df_sup.set_index("Supervisor")["lavadas"], use_container_width=True)
    else:
        st.info("Sin lavados por supervisor para el filtro seleccionado.")

    # --- Faltantes por Supervisor (estimación por segmento/cedis) ---
    st.markdown("**Faltantes estimados por Supervisor**")
    # Definimos unidades "esperadas" por supervisor según CEDIS (y segmento si supervisor lo tiene fijo)
    filas = []
    for sup in sup_by_id.values():
        if cedis_filtro and norm(sup.get("cedis","")) != norm(cedis_filtro):
            continue
        sup_cedis = sup.get("cedis","")
        sup_seg   = sup.get("segmento")  # opcional
        cat_sup = [u for u in catalog_fil if u["cedis"] == sup_cedis]
        if sup_seg:
            cat_sup = [u for u in cat_sup if u["segmento"] == sup_seg]
        total_esp = len(cat_sup)
        lavadas_sup = len([1 for r in regs_fil if r.get("supervisorId")==sup.get("id")])
        faltantes   = max(total_esp - lavadas_sup, 0)
        filas.append({
            "Supervisor": sup.get("nombre", sup.get("id","")),
            "Esperadas": total_esp,
            "Lavadas": lavadas_sup,
            "Faltantes": faltantes
        })
    df_falt = pd.DataFrame(filas)
    if not df_falt.empty:
        st.dataframe(df_falt.sort_values("Faltantes", ascending=False), use_container_width=True)
        st.bar_chart(data=df_falt.set_index("Supervisor")["Faltantes"], use_container_width=True)
    else:
        st.info("No hay datos suficientes para estimar faltantes por supervisor.")

# =============================== App ===============================

def main():
    st.set_page_config(page_title="Lavado semanal", layout="wide")
    ensure_dirs()
    inject_css()

    # Conectar BD
    init_db()
    ok, msg = healthcheck()
    with st.sidebar:
        st.subheader("Estado BD")
        if ok:
            st.success(f"✅ {msg}")
        else:
            st.error(f"❌ {msg}")

    # Header visual
    st.markdown(
        f"""
        <div class="app-header">
          <img class="logo" src="{LOGO_URL}" alt="logo"/>
          <div>
            <div class="title">Lavado semanal de unidades</div>
            <div class="subtitle">Control fotográfico y reportes</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    auth = require_login()      # obliga login
    CATALOGO = load_catalog()
    cedis_labels = {c["id"]: c["nombre"] for c in CONFIG["cedis"]}
    sup_by_id = {s["id"]: s for s in CONFIG["supervisores"]}

    # Barra superior (usuario)
    colH1, colH2 = st.columns([6,1])
    with colH1:
        st.caption(f"Usuario: **{auth['name']}** · Rol: **{auth['role']}**")
    with colH2:
        if st.button("Cerrar sesión"):
            st.session_state.pop("auth", None)
            st.rerun()

    # -------- Filtros superiores --------
    cont = st.container()
    with cont:
        cA, cB, cC, cD = st.columns([1.1, 1.5, 1.6, 1.8])
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
            if sup_seg:
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
                    # hashes locales (evitar misma foto en 2 slots)
                    hashes_local: Dict[str, str] = {}
                    dup_local = False
                    for k,_ in FOTO_SLOTS:
                        h = sha256_bytes(uploads[k].getbuffer())
                        if h in hashes_local.values():
                            dup_local = True
                        hashes_local[k] = h
                    if dup_local:
                        st.error("No podés subir la misma foto en dos posiciones distintas.", icon="🚫")
                        st.stop()

                    # duplicados globales (en BD)
                    all_hashes = photo_hashes_all()  # set de hashes en BD
                    repetidas = [k for k,h in hashes_local.items() if h in all_hashes]
                    if repetidas:
                        st.error(f"Estas fotos ya se usaron antes: {', '.join(repetidas)}.", icon="🚫")
                        st.stop()

                    # guardar fotos en disco
                    fotos_paths = {k: save_photo(uploads[k], k, WEEK, CEDIS, unidad) for k,_ in FOTO_SLOTS}

                    u = next((x for x in CATALOGO if x["id"] == unidad and x["cedis"] == CEDIS), None)
                    record = {
                        "id": uuid.uuid4().hex,
                        "week": WEEK,
                        "cedis": CEDIS,
                        "supervisorId": SUP,
                        "supervisorNombre": (sup_by_id.get(SUP) or {}).get("nombre",""),
                        "unidadId": unidad,
                        "unidadLabel": unidad,
                        "segmento": (u or {}).get("segmento",""),
                        "fotos": fotos_paths,
                        "foto_hashes": hashes_local,
                        "ts": datetime.now().isoformat(timespec="seconds"),
                        "created_by": auth["username"],
                    }

                    # guardar en BD
                    save_lavado(record)
                    st.success("¡Guardado!")
                    st.rerun()

    # -------- Tabla de registros --------
    WEEK_CUR = iso_week_key(fecha_sel)
    st.subheader(f"Registros — {WEEK_CUR}")
    reg_semana = get_lavados_week(WEEK_CUR)  # BD
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
                if p and os.path.exists(p):
                    gcols[i].image(p, use_container_width=True)
                else:
                    gcols[i].write("—")
            cols[5].write(r["ts"])
            can_delete = auth["role"] == "supervisor" and r["supervisorId"] == auth.get("supervisorId")
            if can_delete and cols[6].button("Eliminar", key=r["id"]):
                delete_lavado(r["id"])  # BD
                st.rerun()
            if not can_delete:
                cols[6].write("—")

    # -------- No lavadas --------
    st.subheader(f"Unidades NO lavadas — {WEEK_CUR}")
    if auth["role"] == "supervisor":
        CEDIS_RES = (sup_by_id.get(auth.get("supervisorId") or "", {}) or {}).get("cedis","")
    else:
        CEDIS_RES = CEDIS

    lavadas_set = {(r["unidadId"], r["cedis"]) for r in reg_semana}
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
                }, use_container_width=True)
            else:
                st.success("¡Al día!")

    # -------- Panel administrador --------
    if auth["role"] == "admin":
        st.markdown("---")
        st.header(f"Panel del administrador — {WEEK_CUR}")

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

        # Filtros sobre catálogo y registros
        pool = CATALOGO[:]
        if admin_cedis!="all": pool = [u for u in pool if u["cedis"] == admin_cedis]
        if admin_seg!="all":   pool = [u for u in pool if u["segmento"] == admin_seg]
        if admin_sup!="all":
            ids_asig = {a["unidadId"] for a in CONFIG["asignaciones"] if a["supervisorId"] == admin_sup}
            if ids_asig: pool = [u for u in pool if u["id"] in ids_asig]
        if admin_q.strip():
            q = norm(admin_q)
            pool = [u for u in pool if q in norm(u["id"]) or q in norm(cedis_labels.get(u["cedis"], u["cedis"]))]

        lav = get_lavados_week(WEEK_CUR)  # BD
        if admin_cedis!="all": lav = [r for r in lav if r["cedis"] == admin_cedis]
        if admin_seg!="all":   lav = [r for r in lav if r["segmento"] == admin_seg]
        if admin_sup!="all":   lav = [r for r in lav if r["supervisorId"] == admin_sup]
        if admin_q.strip():
            q = norm(admin_q)
            lav = [r for r in lav if q in norm(r["unidadLabel"]) or q in norm(r["supervisorNombre"])]

        nolav = [u for u in pool if (u["id"], u["cedis"]) not in {(r["unidadId"], r["cedis"]) for r in lav}]

        # XLSX
        xlsx_data = xlsx_week_bytes(WEEK_CUR, lav, nolav)
        st.download_button(
            "Descargar XLSX (lavadas / no lavadas)",
            data=xlsx_data,
            file_name=f"reporte-{WEEK_CUR}.xlsx",
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
                }, use_container_width=True)
            csv_lav = csv_bytes(
                [["week","cedis","supervisor","segmento","unidadId","timestamp","created_by"], *[[
                    WEEK_CUR, r["cedis"], r["supervisorNombre"], r["segmento"], r["unidadLabel"], r["ts"], r.get("created_by","")
                ] for r in lav]]
            )
            st.download_button("Exportar LAVADAS (CSV)", data=csv_lav, file_name=f"lavadas-{WEEK_CUR}.csv", mime="text/csv")

        with cB:
            st.subheader("No lavadas (filtros)")
            st.write(f"Total: {len(nolav)}")
            if nolav:
                st.dataframe({
                    "CEDIS": [cedis_labels.get(u["cedis"], u["cedis"]) for u in nolav],
                    "Segmento": [u["segmento"] for u in nolav],
                    "Unidad": [u["id"] for u in nolav],
                }, use_container_width=True)
            csv_nolav = csv_bytes(
                [["week","cedis","segmento","unidadId"], *[[
                    WEEK_CUR, u["cedis"], u["segmento"], u["id"]
                ] for u in nolav]]
            )
            st.download_button("Exportar NO LAVADAS (CSV)", data=csv_nolav, file_name=f"no-lavadas-{WEEK_CUR}.csv", mime="text/csv")

        st.markdown("---")
        # Export a carpetas + eliminar semana
        cX, cY = st.columns([1,1])
        with cX:
            if st.button("Generar carpetas de la semana (lavados / no_lavados)"):
                export_week_folders(WEEK_CUR, CATALOGO, lav,
                                    only_cedis=None if admin_cedis=="all" else admin_cedis)
                st.success(f"Carpetas listas en {os.path.join(WEEKS_DIR, WEEK_CUR)}")
        with cY:
            if st.button(f"Eliminar TODO la semana {WEEK_CUR}", type="primary"):
                delete_week_everywhere(WEEK_CUR, lav)
                st.success("Semana eliminada (BD + carpetas).")
                st.rerun()

        st.markdown("---")
        admin_user_manager(cedis_labels)

    # -------- Reportes y gráficos globales (al final de main) --------
    st.markdown("---")
    st.header("Reportes y Gráficos")
    cedis_opc = ["(Todos)"] + sorted({u["cedis"] for u in CATALOGO})
    cedis_sel = st.selectbox(
        "Filtrar gráficos por CEDIS",
        options=cedis_opc,
        format_func=lambda x: "Todos" if x == "(Todos)" else cedis_labels.get(x, x),
    )
    cedis_filter = None if cedis_sel == "(Todos)" else cedis_sel
    kpis_y_graficos(
        CATALOGO=CATALOGO,
        reg_semana=reg_semana,
        sup_by_id=sup_by_id,
        cedis_labels=cedis_labels,
        week_key=WEEK_CUR,
        cedis_filtro=cedis_filter,
    )

# ---- run ----
if __name__ == "__main__":
    boot_guard(main)
