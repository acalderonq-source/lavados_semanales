# db.py — capa de datos (SQLAlchemy 2.x) limpia
# ---------------------------------------------
# - Lee DATABASE_URL del entorno (Render/Neon).
# - Fallback a SQLite local si no hay env var.
# - Modelos: User, Lavado.
# - Funciones: init_db, healthcheck, upsert_user, get_user, list_users,
#              save_lavado, delete_lavado, get_lavados_week, photo_hashes_all.

from __future__ import annotations

import os
import json
import datetime
import hashlib
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import (
    create_engine, text, select, delete, String, DateTime, Text, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

# =============== Config de conexión ===============

# En Render/Neon define una env var llamada EXACTAMENTE: DATABASE_URL
# Ejemplo Neon (psycopg2): postgresql+psycopg2://USER:PASSWORD@HOST/neondb?sslmode=require
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    # Fallback local (útil en desarrollo)
    os.makedirs("store", exist_ok=True)
    DATABASE_URL = "sqlite:///store/app.db"

_ENGINE = None
_SessionLocal: Optional[sessionmaker] = None


def get_engine():
    """Crea y reutiliza el engine global."""
    global _ENGINE, _SessionLocal
    if _ENGINE is None:
        _ENGINE = create_engine(
            DATABASE_URL,
            future=True,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        _SessionLocal = sessionmaker(bind=_ENGINE, expire_on_commit=False, future=True)
    return _ENGINE


def get_session():
    """Devuelve una sesión conectada al engine."""
    if _SessionLocal is None:
        get_engine()
    assert _SessionLocal is not None
    return _SessionLocal()


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# =============== Base / Modelos ===============

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    username: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False)  # 'admin' | 'supervisor'
    password_hash: Mapped[str] = mapped_column(String, nullable=False)  # sha256
    supervisor_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Lavado(Base):
    __tablename__ = "lavados"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    week: Mapped[str] = mapped_column(String, index=True)
    cedis: Mapped[str] = mapped_column(String, index=True)
    supervisor_id: Mapped[Optional[str]] = mapped_column(String, index=True)
    supervisor_nombre: Mapped[Optional[str]] = mapped_column(String)
    unidad_id: Mapped[str] = mapped_column(String, index=True)
    unidad_label: Mapped[Optional[str]] = mapped_column(String)
    segmento: Mapped[Optional[str]] = mapped_column(String, index=True)
    ts: Mapped[datetime.datetime] = mapped_column(DateTime, index=True)
    created_by: Mapped[Optional[str]] = mapped_column(String)
    fotos_json: Mapped[Optional[str]] = mapped_column(Text)       # {"frente": "...", ...}
    foto_hashes_json: Mapped[Optional[str]] = mapped_column(Text) # {"frente": "sha256", ...}

    __table_args__ = (
        UniqueConstraint("week", "cedis", "unidad_id", name="uq_week_cedis_unidad"),
    )


# =============== Setup ===============

def init_db() -> None:
    """Crea tablas si no existen."""
    eng = get_engine()
    Base.metadata.create_all(eng)


def healthcheck() -> None:
    """Verifica conectividad básica."""
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))


# =============== USERS (para login en SQL) ===============

def upsert_user(u: Dict[str, Any]) -> None:
    """
    Crea/actualiza usuario.
    Campos: username (obligatorio), name, role ('admin'|'supervisor'),
            sha256 (o password_hash), supervisor_id (opcional).
    """
    with get_session() as s:
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
    with get_session() as s:
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
    with get_session() as s:
        rows = s.execute(select(User)).scalars().all()
        return [
            {
                "username": r.username,
                "name": r.name,
                "role": r.role,
                "supervisor_id": r.supervisor_id,
            }
            for r in rows
        ]


# =============== LAVADOS ===============

def _parse_ts(ts_val: Any) -> datetime.datetime:
    if isinstance(ts_val, datetime.datetime):
        return ts_val
    if isinstance(ts_val, str) and ts_val:
        try:
            return datetime.datetime.fromisoformat(ts_val.replace("Z", "").split("+")[0])
        except Exception:
            pass
    return datetime.datetime.utcnow()


def save_lavado(record: Dict[str, Any]) -> None:
    """
    UPSERT por (week, cedis, unidad_id): elimina el existente y crea uno nuevo.
    record: id, week, cedis, supervisorId, supervisorNombre, unidadId, unidadLabel?,
            segmento, ts?, created_by?, fotos?, foto_hashes?
    """
    with get_session() as s:
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
            supervisor_nombre=record.get("supervisorNombre", ""),
            unidad_id=record["unidadId"],
            unidad_label=record.get("unidadLabel", record["unidadId"]),
            segmento=record.get("segmento", ""),
            ts=_parse_ts(record.get("ts")),
            created_by=record.get("created_by", ""),
            fotos_json=json.dumps(record.get("fotos") or {}),
            foto_hashes_json=json.dumps(record.get("foto_hashes") or {}),
        )
        s.add(row)
        s.commit()


def delete_lavado(lavado_id: str) -> None:
    with get_session() as s:
        s.execute(delete(Lavado).where(Lavado.id == lavado_id))
        s.commit()


def get_lavados_week(week: str) -> List[Dict[str, Any]]:
    with get_session() as s:
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
    with get_session() as s:
        rows = s.execute(select(Lavado.foto_hashes_json)).scalars().all()
        hashes: Set[str] = set()
        for js in rows:
            try:
                for h in (json.loads(js or "{}") or {}).values():
                    if h:
                        hashes.add(h)
            except Exception:
                pass
        return hashes
