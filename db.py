# db.py — SQLAlchemy 2.x para MySQL (Railway) o SQLite local
# Requisitos en requirements.txt:
# SQLAlchemy==2.0.43
# PyMySQL==1.1.0   # para mysql+pymysql://

import os, json, datetime
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import (
    create_engine, text, select, delete,
    String, DateTime, Text, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

# ---------- URL de BD ----------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
# Railway suele dar "mysql://..."; cámbialo a mysql+pymysql://
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

# Fallback local
if not DATABASE_URL:
    os.makedirs("store", exist_ok=True)
    DATABASE_URL = "sqlite:///store/app.db"

# ---------- Engine/Sesión ----------
engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
    pool_recycle=300,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)

# ---------- Base/Modelos ----------
class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    # Longitudes seguras para MySQL (utf8mb4 + índices)
    username: Mapped[str] = mapped_column(String(191), primary_key=True)  # PK
    name: Mapped[str] = mapped_column(String(191), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)         # 'admin'|'supervisor'
    password_hash: Mapped[str] = mapped_column(String(128), nullable=False)  # sha256 hex
    supervisor_id: Mapped[Optional[str]] = mapped_column(String(191), nullable=True)

class Lavado(Base):
    __tablename__ = "lavados"
    id: Mapped[str]                 = mapped_column(String(32), primary_key=True)  # uuid4().hex = 32
    week: Mapped[str]               = mapped_column(String(16), index=True)        # p.ej. 2025-W39
    cedis: Mapped[str]              = mapped_column(String(64), index=True)
    supervisor_id: Mapped[Optional[str]]     = mapped_column(String(191), index=True)
    supervisor_nombre: Mapped[Optional[str]] = mapped_column(String(191))
    unidad_id: Mapped[str]          = mapped_column(String(64), index=True)
    unidad_label: Mapped[Optional[str]] = mapped_column(String(128))
    segmento: Mapped[Optional[str]] = mapped_column(String(32), index=True)
    ts: Mapped[datetime.datetime]   = mapped_column(DateTime, index=True)
    created_by: Mapped[Optional[str]] = mapped_column(String(191))
    fotos_json: Mapped[Optional[str]] = mapped_column(Text)       # JSON serializado
    foto_hashes_json: Mapped[Optional[str]] = mapped_column(Text) # JSON serializado

    __table_args__ = (
        UniqueConstraint("week", "cedis", "unidad_id", name="uq_week_cedis_unidad"),
    )

# ---------- Bootstrap / Utils ----------
def init_db() -> None:
    Base.metadata.create_all(engine)

def healthcheck():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            try:
                ver = conn.execute(text("SELECT VERSION()")).scalar()
            except Exception:
                ver = "desconocida"
            url = engine.url
            host = getattr(url, "host", None) or "?"
            port = getattr(url, "port", None) or "?"
            dbn  = getattr(url, "database", None) or "?"
            return True, f"{url.get_backend_name()} conectado · {host}:{port}/{dbn} · versión {ver}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def _parse_ts(ts_val: Any) -> datetime.datetime:
    if isinstance(ts_val, datetime.datetime):
        return ts_val
    if isinstance(ts_val, str) and ts_val:
        try:
            return datetime.datetime.fromisoformat(ts_val.replace("Z","").split("+")[0])
        except Exception:
            pass
    return datetime.datetime.utcnow()

# ---------- Users ----------
def upsert_user(u: Dict[str, Any]) -> None:
    with SessionLocal() as s:
        row = s.get(User, u["username"])
        if row is None:
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
        if not row:
            return None
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

# ---------- Lavados ----------
def save_lavado(record: Dict[str, Any]) -> None:
    with SessionLocal() as s:
        s.execute(delete(Lavado).where(
            Lavado.week == record["week"],
            Lavado.cedis == record["cedis"],
            Lavado.unidad_id == record["unidadId"],
        ))
        row = Lavado(
            id=record["id"],
            week=record["week"],
            cedis=record["cedis"],
            supervisor_id=record.get("supervisorId"),
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

def delete_lavado(lavado_id: str) -> None:
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

def photo_hashes_all() -> Set[str]:
    with SessionLocal() as s:
        rows = s.execute(select(Lavado.foto_hashes_json)).scalars().all()
        hashes: Set[str] = set()
        for js in rows:
            try:
                for h in (json.loads(js or "{}") or {}).values():
                    if h: hashes.add(h)
            except Exception:
                pass
        return hashes

# Crear tablas
init_db()
