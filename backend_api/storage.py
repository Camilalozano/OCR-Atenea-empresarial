from __future__ import annotations

import json
import os
import time
import base64
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(os.getenv("OCR_ATENEA_DATA_DIR", "data_cases"))
BASE_DIR.mkdir(parents=True, exist_ok=True)

def _case_dir(case_id: str) -> Path:
    d = BASE_DIR / case_id
    d.mkdir(parents=True, exist_ok=True)
    return d

def save_uploads(case_id: str, uploads: List[dict]) -> None:
    """
    uploads: list of {original_name, saved_path, size_bytes, content_type}
    """
    d = _case_dir(case_id)
    meta_path = d / "uploads.json"
    meta_path.write_text(json.dumps({"case_id": case_id, "uploads": uploads}, ensure_ascii=False, indent=2), encoding="utf-8")

def get_uploads(case_id: str) -> List[dict]:
    d = _case_dir(case_id)
    meta_path = d / "uploads.json"
    if not meta_path.exists():
        return []
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    return data.get("uploads", [])

def save_result(case_id: str, result: Dict[str, Any]) -> None:
    """
    result: dict (sin excel bytes binarios, idealmente)
    """
    d = _case_dir(case_id)
    out_path = d / "result.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

def get_result(case_id: str) -> Optional[Dict[str, Any]]:
    d = _case_dir(case_id)
    out_path = d / "result.json"
    if not out_path.exists():
        return None
    return json.loads(out_path.read_text(encoding="utf-8"))

def save_excel(case_id: str, excel_bytes: bytes) -> None:
    d = _case_dir(case_id)
    xls_path = d / "output.xlsx"
    xls_path.write_bytes(excel_bytes)

def get_excel(case_id: str) -> Optional[bytes]:
    d = _case_dir(case_id)
    xls_path = d / "output.xlsx"
    if not xls_path.exists():
        return None
    return xls_path.read_bytes()

def save_status(case_id: str, status: str, extra: Optional[dict] = None) -> None:
    d = _case_dir(case_id)
    s_path = d / "status.json"
    payload = {"case_id": case_id, "status": status, "ts": time.time()}
    if extra:
        payload.update(extra)
    s_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def get_status(case_id: str) -> dict:
    d = _case_dir(case_id)
    s_path = d / "status.json"
    if not s_path.exists():
        return {"case_id": case_id, "status": "not_found"}
    return json.loads(s_path.read_text(encoding="utf-8"))
