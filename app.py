# db.py
# SQLAlchemy + pg8000 para Neon (Postgres)
from typing import Any, Dict, List, Optional
from datetime import datetime

from sqlalchemy import create_engine, text

# Estructura de tabla:
# CREATE TABLE IF NOT EXISTS lavados (
#   id TEXT PRIMARY KEY,
#   week TEXT NOT NULL,
#   cedis TEXT NOT NULL,
#   supervisor_id TEXT,
#   supervisor_nombre TEXT,
#   unidad_id TEXT NOT NULL,
#   segmento TEXT,
#   ts TIMESTAMPTZ,
#   created_by TEXT,
#   fotos JSONB,
#   foto_hashes JSONB
# );

def get_engine(database_url: str):
    # database_url debe ser del tipo: postgresql+pg8000://...
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"ssl": {"cert_reqs": "CERT_REQUIRED"}},
    )
    return engine

def init_db(database_url: str):
    engine = get_engine(database_url)
    ddl = """
    CREATE TABLE IF NOT EXISTS lavados (
        id TEXT PRIMARY KEY,
        week TEXT NOT NULL,
        cedis TEXT NOT NULL,
        supervisor_id TEXT,
        supervisor_nombre TEXT,
        unidad_id TEXT NOT NULL,
        segmento TEXT,
        ts TIMESTAMPTZ,
        created_by TEXT,
        fotos JSONB,
        foto_hashes JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def save_lavado_db(database_url: str, record: Dict[str, Any]):
    """
    Reemplaza si ya existe ese (week, cedis, unidad_id). Genera/respeta record['id'].
    """
    engine = get_engine(database_url)
    # 1) Borrar duplicado lógico (week+cedis+unidad_id)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM lavados WHERE week=:week AND cedis=:cedis AND unidad_id=:unidad_id"),
            {"week": record["week"], "cedis": record["cedis"], "unidad_id": record["unidadId"]},
        )
        # 2) Insertar
        conn.execute(
            text(
                """
                INSERT INTO lavados
                (id, week, cedis, supervisor_id, supervisor_nombre, unidad_id, segmento, ts, created_by, fotos, foto_hashes)
                VALUES
                (:id, :week, :cedis, :supervisor_id, :supervisor_nombre, :unidad_id, :segmento, :ts, :created_by, :fotos::jsonb, :foto_hashes::jsonb)
                """
            ),
            {
                "id": record["id"],
                "week": record["week"],
                "cedis": record["cedis"],
                "supervisor_id": record.get("supervisorId"),
                "supervisor_nombre": record.get("supervisorNombre"),
                "unidad_id": record.get("unidadId"),
                "segmento": record.get("segmento"),
                "ts": record.get("ts") or datetime.utcnow().isoformat(timespec="seconds"),
                "created_by": record.get("created_by"),
                "fotos": json_dumps(record.get("fotos") or {}),
                "foto_hashes": json_dumps(record.get("foto_hashes") or {}),
            },
        )

def get_lavados_week_db(database_url: str, week: str) -> List[Dict[str, Any]]:
    engine = get_engine(database_url)
    sql = text("SELECT * FROM lavados WHERE week=:week ORDER BY ts DESC")
    with engine.begin() as conn:
        rows = conn.execute(sql, {"week": week}).mappings().all()
        res = []
        for r in rows:
            res.append({
                "id": r["id"],
                "week": r["week"],
                "cedis": r["cedis"],
                "supervisorId": r["supervisor_id"],
                "supervisorNombre": r["supervisor_nombre"],
                "unidadId": r["unidad_id"],
                "unidadLabel": r["unidad_id"],
                "segmento": r["segmento"],
                "ts": r["ts"].isoformat() if hasattr(r["ts"], "isoformat") else str(r["ts"]),
                "created_by": r["created_by"],
                "fotos": r["fotos"] or {},
                "foto_hashes": r["foto_hashes"] or {},
            })
        return res

def delete_lavado_db(database_url: str, lavado_id: str):
    engine = get_engine(database_url)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM lavados WHERE id=:id"), {"id": lavado_id})

def delete_week_db(database_url: str, week: str):
    engine = get_engine(database_url)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM lavados WHERE week=:week"), {"week": week})

def photo_hashes_all_db(database_url: str) -> set:
    engine = get_engine(database_url)
    sql = text("SELECT foto_hashes FROM lavados WHERE foto_hashes IS NOT NULL")
    hashes = set()
    with engine.begin() as conn:
        for row in conn.execute(sql):
            d = row[0] or {}
            if isinstance(d, dict):
                for v in d.values():
                    if v:
                        hashes.add(v)
    return hashes

# Utils
import json as _json
def json_dumps(obj: Any) -> str:
    return _json.dumps(obj, ensure_ascii=False)
