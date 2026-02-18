# backend_api/main.py
from __future__ import annotations

import os
import json
import uuid
import mimetypes
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Cliente Supabase (ya lo tienes)
from supabase_config import supabase

# Pipeline
from core_extractor import DocItem, build_openai_client_from_env, run_pipeline

# ✅ Storage (local fallback o Supabase, según variables de entorno)
from storage import (
    save_uploads,
    get_uploads,
    save_result,
    get_result,
    save_excel,
    get_excel,
)

# =========================
# ⚙️ Config
# =========================
APP_NAME = "OCR Atenea Backend API"
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "ocr-atenea").strip()

DATA_DIR = Path(os.environ.get("OCR_ATENEA_DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

TMP_DIR = DATA_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# 🧱 Modelos API
# =========================
class UploadResponse(BaseModel):
    case_id: str
    files_uploaded: List[dict]


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
# 🔧 Helpers
# =========================
def _detect_content_type(filename: str, fallback: str = "application/octet-stream") -> str:
    ctype, _ = mimetypes.guess_type(filename)
    return ctype or fallback


def _tmp_case_dir(case_id: str) -> Path:
    """
    Carpeta temporal donde descargamos PDFs/imagenes desde Supabase
    para que el pipeline (que espera paths locales) pueda procesarlos.
    """
    d = TMP_DIR / case_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _download_from_supabase(storage_path: str) -> bytes:
    """
    Descarga un archivo desde Supabase Storage (bytes).
    storage_path ejemplo: "{case_id}/input/RUT.pdf"
    """
    try:
        return supabase.storage.from_(SUPABASE_BUCKET).download(storage_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error descargando desde Supabase: {storage_path} - {e}")


def _upload_bytes_to_supabase(storage_path: str, content: bytes, content_type: str) -> None:
    """
    Sube bytes a Supabase Storage con upsert.
    """
    try:
        res = supabase.storage.from_(SUPABASE_BUCKET).upload(
            storage_path,
            content,
            {
                "cacheControl": "3600",
                "contentType": content_type or "application/octet-stream",
                "upsert": "true",
            },
        )
        # En algunas versiones res puede ser dict-like
        if isinstance(res, dict) and res.get("error"):
            raise RuntimeError(res.get("error"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error subiendo a Supabase: {storage_path} - {e}")


# =========================
# 🚀 FastAPI
# =========================
app = FastAPI(title=APP_NAME)

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
# 1) 📤 Upload (Supabase Storage + index en storage.save_uploads)
# =========================
@app.post("/upload", response_model=UploadResponse)
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No se recibieron archivos.")

    case_id = uuid.uuid4().hex
    uploads_meta: List[dict] = []

    for f in files:
        original_name = (f.filename or "archivo").strip()
        safe_name = original_name.replace("/", "_").replace("\\", "_")

        content = await f.read()
        content_type = f.content_type or _detect_content_type(safe_name)

        # Guardar en Supabase: {case_id}/input/{filename}
        storage_path = f"{case_id}/input/{safe_name}"
        _upload_bytes_to_supabase(storage_path, content, content_type)

        uploads_meta.append(
            {
                "original_name": original_name,
                "saved_name": safe_name,
                "storage_path": storage_path,
                "content_type": content_type,
                "size_bytes": len(content),
            }
        )

    # ✅ Guardar índice de uploads (en Supabase si está configurado, o local fallback)
    save_uploads(case_id, uploads_meta)

    return UploadResponse(case_id=case_id, files_uploaded=uploads_meta)


# =========================
# 2) 🧠 Process (descarga desde Supabase → temp local → run_pipeline → guarda result/excel)
# =========================
@app.post("/process/{case_id}", response_model=ProcessResponse)
def process(case_id: str):
    uploads = get_uploads(case_id)
    if not uploads:
        raise HTTPException(
            status_code=404,
            detail="case_id no existe o no tiene uploads registrados. Ejecuta /upload primero.",
        )

    # 1) Cliente OpenAI (API KEY en env del backend)
    client = build_openai_client_from_env()

    # 2) Descargar archivos a tmp para procesar (pipeline necesita paths)
    tmp_dir = _tmp_case_dir(case_id)
    items: List[DocItem] = []

    for u in uploads:
        storage_path = u.get("storage_path")
        saved_name = u.get("saved_name") or u.get("original_name") or "archivo"
        content_type = u.get("content_type") or _detect_content_type(saved_name)

        if not storage_path:
            continue

        data = _download_from_supabase(storage_path)
        local_path = tmp_dir / saved_name
        local_path.write_bytes(data)

        items.append(
            DocItem(
                path=str(local_path),
                original_name=str(saved_name),
                content_type=content_type,
            )
        )

    if not items:
        raise HTTPException(status_code=400, detail="No se pudieron preparar archivos para el pipeline.")

    # 3) Ejecutar pipeline
    result: Dict[str, Any] = run_pipeline(items, client)

    # 4) Separar Excel (si viene en el dict)
    excel_bytes = None
    if isinstance(result, dict):
        excel_bytes = result.pop("excel_bytes", None) or result.pop("excel", None)

    # 5) Guardar JSON de resultados (Supabase o local fallback)
    save_result(case_id, result)

    # 6) Guardar Excel (Supabase o local fallback)
    excel_path = None
    if excel_bytes:
        save_excel(case_id, excel_bytes)
        # para “compatibilidad” devolvemos un path lógico
        excel_path = f"{case_id}/output/output.xlsx"

        # (Opcional) además lo dejamos explícito en Supabase con ese nombre
        # storage.py ya lo hace, así que no necesitas duplicarlo aquí.

    # Ruta lógica del JSON en Supabase (o local)
    result_path = f"{case_id}/output/result.json"

    return ProcessResponse(
        case_id=case_id,
        status="processed",
        result_path=result_path,
        excel_path=excel_path,
    )


# =========================
# 3) 📦 Results (desde storage)
# =========================
@app.get("/results/{case_id}")
def get_results_endpoint(case_id: str):
    data = get_result(case_id)
    if not data:
        raise HTTPException(status_code=404, detail="No hay resultados aún. Ejecuta /process/{case_id}.")
    return JSONResponse(data)


# =========================
# 4) 💾 Export (Excel desde storage)
# =========================
@app.get("/export/{case_id}")
def export_excel(case_id: str):
    content = get_excel(case_id)
    if not content:
        raise HTTPException(status_code=404, detail="No existe Excel para este case_id. Ejecuta /process primero.")

    headers = {"Content-Disposition": f'attachment; filename="ocr_atenea_{case_id}.xlsx"'}
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# =========================
# 5) ✅ Approve (humano) - guardamos como JSON usando Supabase directamente
#    (si quieres, lo movemos a storage.py después)
# =========================
def _approval_storage_path(case_id: str) -> str:
    return f"{case_id}/meta/approval.json"


@app.post("/approve/{case_id}")
def approve_case(case_id: str, payload: ApproveRequest):
    approval = {
        "case_id": case_id,
        "approved": payload.approved,
        "reviewer": payload.reviewer,
        "comments": payload.comments,
    }

    # Guardar aprobación en Supabase Storage (meta)
    approval_bytes = json.dumps(approval, ensure_ascii=False, indent=2).encode("utf-8")
    _upload_bytes_to_supabase(_approval_storage_path(case_id), approval_bytes, "application/json")

    return {"status": "ok", "approval": approval}


@app.get("/approve/{case_id}")
def get_approval(case_id: str):
    try:
        data = _download_from_supabase(_approval_storage_path(case_id))
    except HTTPException:
        return {"case_id": case_id, "approved": None}

    approval = json.loads(data.decode("utf-8"))
    return JSONResponse(approval)
