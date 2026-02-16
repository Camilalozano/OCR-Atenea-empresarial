from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# =========================
# ✅ Pega aquí tus funciones actuales
# =========================
# Ejemplos de nombres (ajusta a los tuyos reales):
#
# - extract_rut_fields_raw(pdf_path, logs) -> dict
# - normalizar_campos_rut(raw, logs) -> dict
# - extract_cc_fields_raw(img_or_pdf_path, logs) -> dict
# - normalizar_campos_cc(raw, logs) -> dict
# - extract_doc16_fields_raw(pdf_path, logs) -> dict
# - normalizar_campos_doc16(raw, logs) -> dict
#
# - validar_rut_vs_cedula(rut, cc, logs)
# - validar_cedula_vacia(cc, logs)
# - validar_fecha_certificacion_bancaria(doc16, logs)
#
# - fill_master_values(all_docs: dict, logs) -> pd.DataFrame
# - dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes
#
# - inicializar_logs() -> dict
# - agregar_log(logs, nivel, doc, mensaje, **kwargs)
# - calcular_completitud(doc_type, doc_data) -> float
# - contar_warnings(logs) -> int
#
# =========================

# ---------- LOGS (fallback MVP) ----------
def inicializar_logs() -> Dict[str, Any]:
    return {"items": []}

def agregar_log(logs: Dict[str, Any], nivel: str, doc: str, mensaje: str, **kwargs):
    logs["items"].append({"nivel": nivel, "doc": doc, "mensaje": mensaje, "extra": kwargs})

def contar_warnings(logs: Dict[str, Any]) -> int:
    return sum(1 for x in logs.get("items", []) if str(x.get("nivel", "")).lower() in ("warn", "warning"))

# ---------- OUTPUT EXCEL (fallback MVP) ----------
def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    import io
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="output")
    return bio.getvalue()

# ---------- MASTER DF (fallback MVP) ----------
def fill_master_values(all_docs: Dict[str, dict], logs: Dict[str, Any]) -> pd.DataFrame:
    """
    Crea una tabla simple combinando llaves/valores.
    Tú puedes reemplazar esto por tu tabla maestra real.
    """
    rows = []
    for doc_type, data in all_docs.items():
        if not isinstance(data, dict):
            continue
        for k, v in data.items():
            rows.append({"doc_type": doc_type, "campo": k, "valor": v})
    if not rows:
        return pd.DataFrame(columns=["doc_type", "campo", "valor"])
    return pd.DataFrame(rows)

# ---------- COMPLETITUD (fallback MVP) ----------
def calcular_completitud(doc_type: str, doc_data: dict) -> float:
    if not isinstance(doc_data, dict) or not doc_data:
        return 0.0
    filled = sum(1 for _, v in doc_data.items() if v not in (None, "", [], {}))
    total = max(len(doc_data), 1)
    return round(filled / total, 4)

# =========================
# Clasificación de documentos (MVP)
# =========================

@dataclass
class DocItem:
    path: str
    original_name: str
    content_type: str

def guess_doc_type_by_name(filename: str) -> Optional[str]:
    f = filename.upper()
    # Ajusta estos patrones a tus nombres reales
    if "RUT" in f:
        return "RUT"
    if "CED" in f or "DOC12" in f or "CEDULA" in f or "CC" in f:
        return "CEDULA"
    if "CERTIFICACION" in f or "BANCARIA" in f or "DOC16" in f:
        return "DOC16_CERT_BANCARIA"
    return None

def guess_doc_type_by_text_light(text: str) -> Optional[str]:
    """
    Si más adelante quieres: sacar 1 página de texto y buscar keywords.
    MVP: muy liviano.
    """
    t = (text or "").lower()
    if "registro único tributario" in t or "rut" in t:
        return "RUT"
    if "república de colombia" in t and ("identificación" in t or "cédula" in t):
        return "CEDULA"
    if "certifica" in t and ("banco" in t or "cuenta" in t):
        return "DOC16_CERT_BANCARIA"
    return None

# =========================
# Extractores por tipo (HOOKS)
# =========================

def extract_doc(doc_type: str, path: str, logs: Dict[str, Any]) -> dict:
    """
    Conecta aquí tus extractores reales por documento.
    """
    # ✅ EJEMPLO: reemplaza por tus funciones reales
    if doc_type == "RUT":
        try:
            # raw = extract_rut_fields_raw(path, logs)
            # return normalizar_campos_rut(raw, logs)
            return {"doc_tipo": "RUT", "source_file": os.path.basename(path)}  # placeholder
        except Exception as e:
            agregar_log(logs, "error", "RUT", f"Fallo extracción RUT: {e}")
            return {}
    if doc_type == "CEDULA":
        try:
            # raw = extract_cc_fields_raw(path, logs)
            # return normalizar_campos_cc(raw, logs)
            return {"doc_tipo": "CEDULA", "source_file": os.path.basename(path)}  # placeholder
        except Exception as e:
            agregar_log(logs, "error", "CEDULA", f"Fallo extracción CEDULA: {e}")
            return {}
    if doc_type == "DOC16_CERT_BANCARIA":
        try:
            # raw = extract_doc16_fields_raw(path, logs)
            # return normalizar_campos_doc16(raw, logs)
            return {"doc_tipo": "CERT_BANCARIA", "source_file": os.path.basename(path)}  # placeholder
        except Exception as e:
            agregar_log(logs, "error", "DOC16", f"Fallo extracción DOC16: {e}")
            return {}

    # Para el resto de los 28: irás agregando elif doc_type == "DOCxx": ...
    agregar_log(logs, "warning", doc_type, "No hay extractor implementado aún para este tipo.")
    return {"doc_tipo": doc_type, "source_file": os.path.basename(path)}

# =========================
# Validaciones cruzadas (HOOKS)
# =========================

def cross_validations(all_docs: Dict[str, dict], logs: Dict[str, Any]) -> None:
    """
    Aquí conectas tus validaciones entre documentos.
    """
    rut = all_docs.get("RUT", {})
    cc = all_docs.get("CEDULA", {})
    doc16 = all_docs.get("DOC16_CERT_BANCARIA", {})

    # ✅ Reemplaza por tus validaciones reales
    # validar_rut_vs_cedula(rut, cc, logs)
    # validar_cedula_vacia(cc, logs)
    # validar_fecha_certificacion_bancaria(doc16, logs)

    # MVP: ejemplo de warning si faltan
    if not rut:
        agregar_log(logs, "warning", "RUT", "No se cargó o no se pudo extraer RUT.")
    if not cc:
        agregar_log(logs, "warning", "CEDULA", "No se cargó o no se pudo extraer Cédula.")
    if doc16 and not isinstance(doc16, dict):
        agregar_log(logs, "warning", "DOC16", "DOC16 no tiene estructura dict esperada.")

# =========================
# ✅ run_pipeline() principal
# =========================

def run_pipeline(items: List[DocItem]) -> Dict[str, Any]:
    """
    Procesa N documentos.
    Retorna:
      - all_docs: dict doc_type -> data dict
      - metricas: por documento y totales
      - logs
      - df_master (serializado)
      - excel_bytes
    """
    logs = inicializar_logs()
    t0 = time.time()

    # 1) Clasificar por nombre (MVP)
    classified: List[Tuple[str, DocItem]] = []
    unclassified: List[DocItem] = []

    for it in items:
        doc_type = guess_doc_type_by_name(it.original_name)
        if doc_type:
            classified.append((doc_type, it))
        else:
            unclassified.append(it)

    # 2) Para no clasificados: marcar como "UNKNOWN"
    for it in unclassified:
        classified.append(("UNKNOWN", it))
        agregar_log(logs, "warning", "UNKNOWN", f"No se pudo inferir tipo de documento por nombre: {it.original_name}")

    # 3) Extraer por documento (con métricas por doc)
    all_docs: Dict[str, dict] = {}
    metricas_por_doc: List[dict] = []

    for doc_type, it in classified:
        doc_t0 = time.time()
        data = extract_doc(doc_type, it.path, logs)

        # Guardar (si llegan duplicados del mismo tipo, los apilamos)
        if doc_type in all_docs:
            # convierte a lista si hay múltiples del mismo tipo
            prev = all_docs[doc_type]
            if isinstance(prev, list):
                prev.append(data)
                all_docs[doc_type] = prev
            else:
                all_docs[doc_type] = [prev, data]
        else:
            all_docs[doc_type] = data

        metricas_por_doc.append({
            "doc_type": doc_type,
            "file": it.original_name,
            "tiempo_seg": round(time.time() - doc_t0, 2),
            "completitud": calcular_completitud(doc_type, data if isinstance(data, dict) else {}),
        })

    # 4) Validaciones cruzadas (solo si estructura esperada)
    # Para simplificar: si hay listas en all_docs, tu versión enterprise debería decidir “cuál” validar.
    try:
        cross_validations(all_docs, logs)
    except Exception as e:
        agregar_log(logs, "error", "CROSS_VALIDATIONS", f"Fallo validaciones cruzadas: {e}")

    # 5) Master y Excel
    df_master = fill_master_values(all_docs, logs)
    excel_bytes = dataframe_to_excel_bytes(df_master)

    total_time = round(time.time() - t0, 2)
    metricas = {
        "tiempo_total_seg": total_time,
        "warnings_total": contar_warnings(logs),
        "docs_procesados": len(metricas_por_doc),
        "por_documento": metricas_por_doc,
    }

    return {
        "all_docs": all_docs,
        "metricas": metricas,
        "logs": logs,
        "df_master": df_master.to_dict(orient="records"),
        "excel_bytes": excel_bytes,  # el backend lo guarda en storage; en JSON no lo devolvemos crudo
    }
