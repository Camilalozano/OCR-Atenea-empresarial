# backend_api/main.py
from __future__ import annotations

import os
import json
import uuid
import shutil
import mimetypes
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ✅ Importa tu pipeline REAL (core_extractor.py)
# Debe exponer:
# - DocItem (dataclass o pydantic)
# - build_openai_client_from_env()
# - run_pipeline(items, client) -> dict con result + excel_bytes (o excel_bytes aparte)
from core_extractor import DocItem, build_openai_client_from_env, run_pipeline


# =========================
# ⚙️ Config
# =========================
APP_NAME = "OCR Atenea Backend API"
DATA_DIR = Path(os.environ.get("OCR_ATENEA_DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

UPLOADS_DIR = DATA_DIR / "uploads"
RESULTS_DIR = DATA_DIR / "results"
EXCEL_DIR = DATA_DIR / "excel"
APPROVALS_DIR = DATA_DIR / "approvals"

for d in [UPLOADS_DIR, RESULTS_DIR, EXCEL_DIR, APPROVALS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# =========================
# 🧱 Modelos API
# =========================
class UploadResponse(BaseModel):
    case_id: str
    files: List[dict]


class ProcessResponse(BaseModel):
    case_id: str
    status: str
    result_path: str
    excel_path: Optional[str] = None


class ApproveRequest(BaseModel):
    approved: bool
    reviewer: Optional[str] = None
    comments: Optional[str] = None


# =========================
# 🔧 Helpers de storage (local “enterprise-lite”)
# =========================
def _case_dir(case_id: str) -> Path:
    return UPLOADS_DIR / case_id


def _result_json_path(case_id: str) -> Path:
    return RESULTS_DIR / f"{case_id}.json"


def _excel_path(case_id: str) -> Path:
    return EXCEL_DIR / f"{case_id}.xlsx"


def _approval_path(case_id: str) -> Path:
    return APPROVALS_DIR / f"{case_id}.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _detect_content_type(filename: str, fallback: str = "application/octet-stream") -> str:
    ctype, _ = mimetypes.guess_type(filename)
    return ctype or fallback


def _list_uploaded_files(case_id: str) -> List[Path]:
    cdir = _case_dir(case_id)
    if not cdir.exists():
        return []
    return sorted([p for p in cdir.glob("*") if p.is_file()])


# =========================
# 🚀 FastAPI
# =========================
app = FastAPI(title=APP_NAME)

# Si vas a llamar desde Streamlit en otro dominio, esto te evita bloqueos CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # en prod: cambia por tus dominios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "service": APP_NAME}


# =========================
# 1) 📤 Upload (N archivos)
# =========================
@app.post("/upload", response_model=UploadResponse)
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No se recibieron archivos.")

    case_id = uuid.uuid4().hex
    cdir = _case_dir(case_id)
    cdir.mkdir(parents=True, exist_ok=True)

    saved = []
    for f in files:
        # Guardamos con nombre “seguro” (evita path traversal)
        original_name = (f.filename or "archivo").strip()
        safe_name = original_name.replace("/", "_").replace("\\", "_")
        out_path = cdir / safe_name

        with out_path.open("wb") as w:
            shutil.copyfileobj(f.file, w)

        saved.append(
            {
                "original_name": original_name,
                "saved_name": safe_name,
                "path": str(out_path),
                "content_type": f.content_type or _detect_content_type(original_name),
                "size_bytes": out_path.stat().st_size,
            }
        )

    return UploadResponse(case_id=case_id, files=saved)


# =========================
# 2) 🧠 Process (pipeline)
# =========================
@app.post("/process/{case_id}", response_model=ProcessResponse)
def process(case_id: str):
    uploaded = _list_uploaded_files(case_id)
    if not uploaded:
        raise HTTPException(status_code=404, detail="case_id no existe o no tiene archivos.")

    # ✅ 1) Cliente OpenAI desde variables de entorno (tokens empresariales)
    # Recomendado: OPENAI_API_KEY en env del backend (NO en el frontend)
    client = build_openai_client_from_env()

    # ✅ 2) Construir items (uno por archivo)
    items: List[DocItem] = []
    for p in uploaded:
        items.append(
            DocItem(
                path=str(p),
                original_name=p.name,
                content_type=_detect_content_type(p.name),
            )
        )

    # ✅ 3) Correr pipeline real
    # Esperado: dict con:
    # - "data" / "tables" / "metricas" / "logs" ... lo que definas
    # - "excel_bytes" (bytes) o "excel" (bytes)
    result: Dict[str, Any] = run_pipeline(items, client)

    # ✅ 4) Guardar resultados (JSON + Excel por separado)
    excel_bytes = None
    if isinstance(result, dict):
        # acepta cualquier convención razonable
        excel_bytes = result.pop("excel_bytes", None) or result.pop("excel", None)

    # Guardar JSON
    result_path = _result_json_path(case_id)
    _write_json(result_path, result)

    excel_path = None
    if excel_bytes:
        excel_path = _excel_path(case_id)
        excel_path.write_bytes(excel_bytes)

    return ProcessResponse(
        case_id=case_id,
        status="processed",
        result_path=str(result_path),
        excel_path=str(excel_path) if excel_path else None,
    )


# =========================
# 3) 📦 Results
# =========================
@app.get("/results/{case_id}")
def get_results(case_id: str):
    path = _result_json_path(case_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="No hay resultados aún. Ejecuta /process/{case_id}.")
    return JSONResponse(_read_json(path))


# =========================
# 4) 💾 Export (Excel)
# =========================
@app.get("/export/{case_id}")
def export_excel(case_id: str):
    path = _excel_path(case_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="No existe Excel para este case_id. Ejecuta /process primero.")
    content = path.read_bytes()
    headers = {"Content-Disposition": f'attachment; filename="ocr_atenea_{case_id}.xlsx"'}
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# =========================
# 5) ✅ Approve (humano)
# =========================
@app.post("/approve/{case_id}")
def approve_case(case_id: str, payload: ApproveRequest):
    # Si quieres, aquí también puedes exigir que exista resultado previo:
    # if not _result_json_path(case_id).exists(): ...

    approval = {
        "case_id": case_id,
        "approved": payload.approved,
        "reviewer": payload.reviewer,
        "comments": payload.comments,
    }
    _write_json(_approval_path(case_id), approval)
    return {"status": "ok", "approval": approval}


@app.get("/approve/{case_id}")
def get_approval(case_id: str):
    path = _approval_path(case_id)
    if not path.exists():
        return {"case_id": case_id, "approved": None}
    return JSONResponse(_read_json(path))
