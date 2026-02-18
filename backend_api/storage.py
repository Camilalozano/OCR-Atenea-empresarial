from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# ============================================================
# 💾 Storage abstraction (Local fallback + Supabase Storage)
# - Si SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY existen -> usa Supabase
# - Si no existen -> usa filesystem local (como tu piloto actual)
# ============================================================

# -------- Local (fallback) --------
BASE_DIR = Path(os.getenv("OCR_ATENEA_DATA_DIR", "data_cases"))
BASE_DIR.mkdir(parents=True, exist_ok=True)


def _case_dir(case_id: str) -> Path:
    d = BASE_DIR / case_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# -------- Supabase config --------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "ocr-atenea").strip()

# Rutas dentro del bucket (estándar recomendado)
# ocr-atenea/{case_id}/meta/...
# ocr-atenea/{case_id}/output/...
# ocr-atenea/{case_id}/status/...
def _sb_path(case_id: str, *parts: str) -> str:
    safe_parts = [p.strip("/").replace("\\", "/") for p in parts if p]
    return "/".join([case_id] + safe_parts)


def _use_supabase() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


def _get_supabase_client():
    """
    Crea cliente Supabase SOLO si hay variables de entorno.
    """
    if not _use_supabase():
        return None
    try:
        from supabase import create_client  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "No se pudo importar supabase. ¿Instalaste `supabase>=2.0.0`?"
        ) from e
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _sb_upload_bytes(path: str, content: bytes, content_type: str = "application/octet-stream") -> None:
    """
    Sube bytes a Supabase Storage. Usa upsert (sobrescribe).
    """
    sb = _get_supabase_client()
    if sb is None:
        raise RuntimeError("Supabase no está configurado (faltan variables SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY).")

    # ⚠️ En algunas versiones, file_options espera strings
    file_options = {"content-type": content_type, "upsert": "true"}

    res = sb.storage.from_(SUPABASE_BUCKET).upload(
        path=path,
        file=content,
        file_options=file_options,
    )

    # Dependiendo de versión, res puede ser dict-like o tener .get(...)
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(f"Supabase upload error: {res.get('error')}")


def _sb_download_bytes(path: str) -> bytes:
    """
    Descarga bytes desde Supabase Storage.
    """
    sb = _get_supabase_client()
    if sb is None:
        raise RuntimeError("Supabase no está configurado (faltan variables SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY).")

    data = sb.storage.from_(SUPABASE_BUCKET).download(path)
    # Normalmente devuelve bytes
    if data is None:
        raise FileNotFoundError(f"No se encontró el archivo en Supabase: {path}")
    return data


def _sb_write_json(path: str, payload: dict) -> None:
    content = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    _sb_upload_bytes(path, content, content_type="application/json")


def _sb_read_json(path: str) -> Optional[dict]:
    try:
        content = _sb_download_bytes(path)
    except FileNotFoundError:
        return None
    return json.loads(content.decode("utf-8"))


# ============================================================
# ✅ API pública que usa tu backend (misma firma que tu piloto)
# ============================================================

def save_uploads(case_id: str, uploads: List[dict]) -> None:
    """
    uploads: list of {original_name, saved_path, size_bytes, content_type}
    - En modo Supabase: guarda meta en {case_id}/meta/uploads.json
    - En modo local: guarda en data_cases/{case_id}/uploads.json (igual que antes)
    """
    payload = {"case_id": case_id, "uploads": uploads}

    if _use_supabase():
        _sb_write_json(_sb_path(case_id, "meta", "uploads.json"), payload)
        return

    # 🧱 Fallback local
    d = _case_dir(case_id)
    meta_path = d / "uploads.json"
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_uploads(case_id: str) -> List[dict]:
    if _use_supabase():
        data = _sb_read_json(_sb_path(case_id, "meta", "uploads.json"))
        if not data:
            return []
        return data.get("uploads", [])

    # 🧱 Fallback local
    d = _case_dir(case_id)
    meta_path = d / "uploads.json"
    if not meta_path.exists():
        return []
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    return data.get("uploads", [])


def save_result(case_id: str, result: Dict[str, Any]) -> None:
    """
    result: dict (idealmente sin excel bytes binarios)
    - Supabase: {case_id}/output/result.json
    - Local: data_cases/{case_id}/result.json
    """
    if _use_supabase():
        _sb_write_json(_sb_path(case_id, "output", "result.json"), result)
        return

    # 🧱 Fallback local
    d = _case_dir(case_id)
    out_path = d / "result.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def get_result(case_id: str) -> Optional[Dict[str, Any]]:
    if _use_supabase():
        return _sb_read_json(_sb_path(case_id, "output", "result.json"))

    # 🧱 Fallback local
    d = _case_dir(case_id)
    out_path = d / "result.json"
    if not out_path.exists():
        return None
    return json.loads(out_path.read_text(encoding="utf-8"))


def save_excel(case_id: str, excel_bytes: bytes) -> None:
    """
    - Supabase: {case_id}/output/output.xlsx
    - Local: data_cases/{case_id}/output.xlsx
    """
    if _use_supabase():
        _sb_upload_bytes(_sb_path(case_id, "output", "output.xlsx"), excel_bytes,
                         content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        return

    # 🧱 Fallback local
    d = _case_dir(case_id)
    xls_path = d / "output.xlsx"
    xls_path.write_bytes(excel_bytes)


def get_excel(case_id: str) -> Optional[bytes]:
    if _use_supabase():
        try:
            return _sb_download_bytes(_sb_path(case_id, "output", "output.xlsx"))
        except FileNotFoundError:
            return None

    # 🧱 Fallback local
    d = _case_dir(case_id)
    xls_path = d / "output.xlsx"
    if not xls_path.exists():
        return None
    return xls_path.read_bytes()


def save_status(case_id: str, status: str, extra: Optional[dict] = None) -> None:
    """
    - Supabase: {case_id}/status/status.json
    - Local: data_cases/{case_id}/status.json
    """
    payload = {"case_id": case_id, "status": status, "ts": time.time()}
    if extra:
        payload.update(extra)

    if _use_supabase():
        _sb_write_json(_sb_path(case_id, "status", "status.json"), payload)
        return

    # 🧱 Fallback local
    d = _case_dir(case_id)
    s_path = d / "status.json"
    s_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_status(case_id: str) -> dict:
    if _use_supabase():
        data = _sb_read_json(_sb_path(case_id, "status", "status.json"))
        if not data:
            return {"case_id": case_id, "status": "not_found"}
        return data

    # 🧱 Fallback local
    d = _case_dir(case_id)
    s_path = d / "status.json"
    if not s_path.exists():
        return {"case_id": case_id, "status": "not_found"}
    return json.loads(s_path.read_text(encoding="utf-8"))
