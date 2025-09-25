# db.py
import os, json, datetime
from typing import Any, Dict, List, Optional
from sqlalchemy import (
    create_engine, Column, String, DateTime, Text,
    UniqueConstraint, select, delete
)
from sqlalchemy.orm import declarative_base, sessionmaker

# db.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DATABASE_URL = os.getenv("DATABASE_URL", "")

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        print(f"[db] Creating engine... ({DATABASE_URL[:70]}...)")
        # pool_pre_ping evita conexiones muertas; timeout para pg8000:
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"timeout": 10},  # pg8000 timeout
        )
    return _engine

def init_db():
    eng = get_engine()
    try:
        with eng.begin() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS lavados(
              id TEXT PRIMARY KEY,
              week TEXT,
              cedis TEXT,
              supervisor_id TEXT,
              supervisor_nombre TEXT,
              unidad_id TEXT,
              segmento TEXT,
              ts TIMESTAMP,
              created_by TEXT
            )
            """))
        print("[db] init_db OK")
    except SQLAlchemyError as e:
        print("[db] init_db FAILED:", repr(e))
        raise

def healthcheck():
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("[db] healthcheck OK")

# ========= Conexión =========
# 1) Primero ENV (ideal para Neon):
DATABASE_URL = os.getenv("psql 'postgresql://neondb_owner:npg_3BMrm8QFDycs@ep-sparkling-water-ad1bjit1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'", "")

# 2) Si corres en Streamlit Cloud, también puede venir de secrets:
if not DATABASE_URL:
    try:
        import streamlit as st  # type: ignore
        DATABASE_URL = st.secrets["psql 'postgresql://neondb_owner:npg_3BMrm8QFDycs@ep-sparkling-water-ad1bjit1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'"]
    except Exception:
        DATABASE_URL = ""

# 3) Fallback local (desarrollo):
if not DATABASE_URL:
    os.makedirs("store", exist_ok=True)
    DATABASE_URL = "sqlite:///store/app.db"

# Para Neon debes usar el driver psycopg2 y sslmode=require en la URL:
# postgresql+psycopg2://user:pass@host/db?sslmode=require

engine = create_engine(
    DATABASE_URL,
    future=True,
    echo=False,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
Base = declarative_base()

# ========= Tablas =========

class User(Base):
    __tablename__ = "users"
    username        = Column(String, primary_key=True)
    name            = Column(String, nullable=False)
    role            = Column(String, nullable=False)   # 'admin' | 'supervisor'
    password_hash   = Column(String, nullable=False)   # sha256
    supervisor_id   = Column(String, nullable=True)

class Lavado(Base):
    __tablename__ = "lavados"
    id                = Column(String, primary_key=True)
    week              = Column(String, index=True)
    cedis             = Column(String, index=True)
    supervisor_id     = Column(String, index=True)
    supervisor_nombre = Column(String)
    unidad_id         = Column(String, index=True)
    unidad_label      = Column(String)
    segmento          = Column(String, index=True)
    ts                = Column(DateTime, index=True)
    created_by        = Column(String)
    fotos_json        = Column(Text)      # {"frente":path,...}
    foto_hashes_json  = Column(Text)      # {"frente":hash,...}

    __table_args__ = (
        UniqueConstraint('week','cedis','unidad_id', name='uq_week_cedis_unidad'),
    )

# ========= Setup =========

def init_db():
    """Crea las tablas si no existen."""
    Base.metadata.create_all(engine)

# ========= USERS (opcional, por si migras login a SQL) =========

def upsert_user(u: Dict[str, Any]):
    with SessionLocal() as s:
        row = s.get(User, u["username"])
        if not row:
            row = User(username=u["username"])
            s.add(row)
        row.name = u.get("name") or u.get("nombre") or u["username"]
        row.role = u.get("role", "supervisor")
        row.password_hash = u.get("sha256") or u.get("password_hash") or ""
        row.supervisor_id = u.get("supervisor_id")
        s.commit()

def get_user(username: str) -> Optional[Dict[str, Any]]:
    with SessionLocal() as s:
        row = s.get(User, username)
        if not row: return None
        return {
            "username": row.username,
            "name": row.name,
            "role": row.role,
            "sha256": row.password_hash,
            "supervisor_id": row.supervisor_id,
        }

def list_users() -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        rows = s.execute(select(User)).scalars().all()
        return [{
            "username": r.username,
            "name": r.name,
            "role": r.role,
            "supervisor_id": r.supervisor_id
        } for r in rows]

# ========= LAVADOS =========

def _parse_ts(ts_val: Any) -> datetime.datetime:
    if isinstance(ts_val, datetime.datetime):
        return ts_val
    if isinstance(ts_val, str) and ts_val:
        # quita zona si viene con +00:00
        try:
            return datetime.datetime.fromisoformat(ts_val.replace("Z","").split("+")[0])
        except Exception:
            pass
    return datetime.datetime.utcnow()

def save_lavado(record: Dict[str, Any]):
    """
    UPSERT por (week, cedis, unidad_id).
    Borra el existente y luego inserta uno nuevo con el mismo (week, cedis, unidad).
    """
    with SessionLocal() as s:
        s.execute(delete(Lavado).where(
            Lavado.week == record["week"],
            Lavado.cedis == record["cedis"],
            Lavado.unidad_id == record["unidadId"]
        ))
        row = Lavado(
            id=record["id"],
            week=record["week"],
            cedis=record["cedis"],
            supervisor_id=record["supervisorId"],
            supervisor_nombre=record.get("supervisorNombre",""),
            unidad_id=record["unidadId"],
            unidad_label=record.get("unidadLabel", record["unidadId"]),
            segmento=record.get("segmento",""),
            ts=_parse_ts(record.get("ts")),
            created_by=record.get("created_by",""),
            fotos_json=json.dumps(record.get("fotos") or {}),
            foto_hashes_json=json.dumps(record.get("foto_hashes") or {}),
        )
        s.add(row)
        s.commit()

def delete_lavado(lavado_id: str):
    with SessionLocal() as s:
        s.execute(delete(Lavado).where(Lavado.id == lavado_id))
        s.commit()

def get_lavados_week(week: str) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        rows = s.execute(
            select(Lavado).where(Lavado.week == week).order_by(Lavado.ts.desc())
        ).scalars().all()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "id": r.id,
                "week": r.week,
                "cedis": r.cedis,
                "supervisorId": r.supervisor_id,
                "supervisorNombre": r.supervisor_nombre,
                "unidadId": r.unidad_id,
                "unidadLabel": r.unidad_label,
                "segmento": r.segmento,
                "fotos": json.loads(r.fotos_json or "{}"),
                "foto_hashes": json.loads(r.foto_hashes_json or "{}"),
                "ts": (r.ts or datetime.datetime.utcnow()).isoformat(timespec="seconds"),
                "created_by": r.created_by,
            })
        return out

def photo_hashes_all() -> set:
    with SessionLocal() as s:
        rows = s.execute(select(Lavado.foto_hashes_json)).scalars().all()
        hashes = set()
        for js in rows:
            try:
                for h in (json.loads(js or "{}") or {}).values():
                    if h: hashes.add(h)
            except Exception:
                pass
        return hashes
