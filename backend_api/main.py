from __future__ import annotations

import os
import uuid
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.responses import JSONResponse, StreamingResponse

from core_extractor import DocItem, run_pipeline
from storage import (
    save_uploads, get_uploads,
    save_result, get_result,
    save_excel, get_excel,
    save_status, get_status
)

app = FastAPI(title="OCR Atenea Backend")

MAX_FILES = int(os.getenv("OCR_ATENEA_MAX_FILES", "40"))  # margen sobre 28
MAX_MB = int(os.getenv("OCR_ATENEA_MAX_MB_PER_FILE", "25"))

def _case_dir(case_id: str) -> str:
    base = os.getenv("OCR_ATENEA_DATA_DIR", "data_cases")
    return os.path.join(base, case_id)

@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")
    if len(files) > MAX_FILES:
        raise HTTPException(status_code=400, detail=f"Too many files. Max={MAX_FILES}")

    case_id = str(uuid.uuid4())
    os.makedirs(_case_dir(case_id), exist_ok=True)

    uploads_meta = []
    for f in files:
        content = await f.read()
        size_mb = len(content) / (1024 * 1024)
        if size_mb > MAX_MB:
            raise HTTPException(status_code=400, detail=f"File too large: {f.filename} ({size_mb:.1f}MB). Max={MAX_MB}MB")

        saved_path = os.path.join(_case_dir(case_id), f.filename)
        with open(saved_path, "wb") as out:
            out.write(content)

        uploads_meta.append({
            "original_name": f.filename,
            "saved_path": saved_path,
            "size_bytes": len(content),
            "content_type": f.content_type or "application/octet-stream",
        })

    save_uploads(case_id, uploads_meta)
    save_status(case_id, "uploaded", {"n_files": len(uploads_meta)})
    return {"case_id": case_id, "n_files": len(uploads_meta)}

@app.post("/process/{case_id}")
def process(case_id: str):
    uploads = get_uploads(case_id)
    if not uploads:
        raise HTTPException(status_code=404, detail="case_id not found or no uploads")

    save_status(case_id, "processing")

    items = [
        DocItem(
            path=u["saved_path"],
            original_name=u["original_name"],
            content_type=u.get("content_type", "application/octet-stream"),
        )
        for u in uploads
    ]

    result = run_pipeline(items)

    # Guardar excel por separado (no meter binarios en result.json)
    excel_bytes = result.pop("excel_bytes", b"")
    if excel_bytes:
        save_excel(case_id, excel_bytes)

    save_result(case_id, result)
    save_status(case_id, "processed", {"docs_procesados": result.get("metricas", {}).get("docs_procesados", 0)})

    return {"case_id": case_id, "status": "processed", "metricas": result.get("metricas", {})}

@app.get("/results/{case_id}")
def results(case_id: str):
    res = get_result(case_id)
    if not res:
        raise HTTPException(status_code=404, detail="case_id not found or not processed")
    status = get_status(case_id)
    return JSONResponse({"case_id": case_id, "status": status, "result": res})

@app.get("/export/{case_id}")
def export(case_id: str):
    data = get_excel(case_id)
    if not data:
        raise HTTPException(status_code=404, detail="Excel not found. Process the case first.")
    filename = f"ocr_atenea_{case_id}.xlsx"
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.post("/approve/{case_id}")
def approve(case_id: str, payload: dict = Body(...)):
    res = get_result(case_id)
    if not res:
        raise HTTPException(status_code=404, detail="case_id not found or not processed")

    # payload ejemplo: {"approved": true, "approved_by": "user@x.com", "notes": "..."}
    res["aprobacion"] = payload
    save_result(case_id, res)
    save_status(case_id, "approved", {"approved": payload.get("approved", None)})

    return {"case_id": case_id, "status": "approved_saved"}
