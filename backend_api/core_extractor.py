# backend_api/core_extractor.py
from __future__ import annotations

import os
import re
import json
import io
import time
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional

import fitz  # PyMuPDF
import numpy as np
import pandas as pd
from PIL import Image
import easyocr
from openai import OpenAI


# =========================
# 📦 Tipos / Entrada
# =========================
@dataclass
class DocItem:
    path: str
    original_name: str
    content_type: str = "application/pdf"


# =========================
# ⚠️ Logging / Validaciones
# =========================
def inicializar_logs():
    return []

def agregar_log(logs: list, documento: str, tipo: str, mensaje: str):
    """tipo: INFO | WARNING | ERROR"""
    logs.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "documento": documento,
        "tipo": tipo,
        "mensaje": mensaje
    })
    return logs


# =========================
# 💾 Utilidades generales
# =========================
def safe_json_loads(raw: str) -> dict:
    raw = (raw or "").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)

def only_digits(x: str | None) -> str | None:
    if x is None:
        return None
    d = re.sub(r"\D", "", str(x))
    return d if d else None

def normalize_text(x: str | None) -> str | None:
    if x is None:
        return None
    x = str(x).strip()
    return x if x else None

def normalize_date(x: str | None) -> str | None:
    """
    Deja fechas como texto, pero intenta normalizar un poco.
    Acepta ejemplos tipo 16-OCT-1986 / 12-NOV-2004 / 2004-11-12
    """
    if not x:
        return None
    x = str(x).strip().upper()

    # Si ya viene ISO-ish
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", x):
        return x

    # dd-MMM-yyyy
    m = re.search(r"(\d{1,2})[-/ ]([A-Z]{3})[-/ ](\d{4})", x)
    if m:
        dd = int(m.group(1))
        mon = m.group(2)
        yyyy = int(m.group(3))
        months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        if mon in months:
            return f"{yyyy:04d}-{months[mon]:02d}-{dd:02d}"

    # dd-mm-yyyy
    m = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", x)
    if m:
        dd = int(m.group(1))
        mm = int(m.group(2))
        yyyy = int(m.group(3))
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    return x  # fallback


def limpiar_texto_para_llm(text: str) -> str:
    """
    Limpia texto extraído de PDF para evitar UnicodeEncodeError:
    - Normaliza Unicode
    - Elimina caracteres de control e invisibles
    - Reemplaza espacios raros por espacios normales
    """
    if not text:
        return ""

    # Normaliza (quita rarezas tipo ligaduras)
    t = unicodedata.normalize("NFKC", text)

    # Reemplazar espacios raros
    t = t.replace("\u00A0", " ")  # non-breaking space
    t = t.replace("\u200B", "")   # zero-width space
    t = t.replace("\u200E", "")   # LRM
    t = t.replace("\u200F", "")   # RLM

    # Eliminar caracteres de control (excepto saltos de línea y tab)
    cleaned = []
    for ch in t:
        cat = unicodedata.category(ch)
        if cat.startswith("C") and ch not in ["\n", "\t"]:
            continue
        cleaned.append(ch)

    t = "".join(cleaned)

    # Compactar espacios
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)

    return t.strip()


# =========================
# ✅ Mejora RUT: anti-código-de-barras (numero_identificacion)
# =========================
@lru_cache(maxsize=1)
def get_easyocr_reader():
    return easyocr.Reader(["es"], gpu=False)

# alias por compatibilidad
get_ocr_reader = get_easyocr_reader


def ocr_numero_identificacion_desde_campo26(pdf_bytes: bytes) -> str | None:
    """
    Busca en el PDF el texto 'Número de Identificación' y hace OCR SOLO en un recorte
    cerca de ese campo para capturar la cédula correcta (8-10 dígitos).
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    reader = get_easyocr_reader()

    targets = ["Número de Identificación", "Numero de Identificacion"]

    for page in doc:
        rects = []
        for t in targets:
            rects += page.search_for(t)

        if not rects:
            continue

        r = rects[0]

        clip = fitz.Rect(
            r.x0,
            max(r.y0 - 20, 0),
            min(r.x1 + 350, page.rect.x1),
            min(r.y1 + 80, page.rect.y1)
        )

        pix = page.get_pixmap(clip=clip, dpi=300)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        results = reader.readtext(np.array(img), detail=0)

        candidatos = []
        for s in results:
            dig = re.sub(r"\D", "", s)
            if 8 <= len(dig) <= 10:
                candidatos.append(dig)

        if candidatos:
            candidatos.sort(key=len, reverse=True)
            return candidatos[0]

    return None


def numero_id_es_sospechoso(num: str | None) -> bool:
    if not num:
        return True
    num = re.sub(r"\D", "", str(num))
    if not (8 <= len(num) <= 10):
        return True
    return False


def extraer_numero_identificacion_regla(texto: str) -> str | None:
    """
    Intenta extraer el campo 26. Número de Identificación (CC) del RUT.
    Evita confundirlo con:
      - 4. Número de formulario
      - 5. NIT
    """
    t = " ".join(texto.split())

    m = re.search(r"26\.\s*Número de Identificación\s*([0-9\s]{6,20})", t, re.IGNORECASE)
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 6 <= len(cand) <= 11:
            return cand

    m = re.search(r"Cédula de Ciudadanía\s*([0-9\s]{6,20})", t, re.IGNORECASE)
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 6 <= len(cand) <= 11:
            return cand

    return None


def corregir_numero_identificacion(data: dict, texto_pdf: str) -> dict:
    """
    Si la IA se equivoca, reemplaza numero_identificacion por el detectado en el PDF.
    """
    regla = extraer_numero_identificacion_regla(texto_pdf)
    if regla:
        data["numero_identificacion"] = regla
    return data


def validar_numero_identificacion(texto: str, candidato: str) -> str | None:
    """
    Valida que el número venga del campo 26 del RUT
    """
    if not candidato:
        return None

    candidato = re.sub(r"\D", "", candidato)

    if not (8 <= len(candidato) <= 10):
        return None

    patron = re.compile(r"26\.\s*Número de Identificación\s*[\n: ]+\s*(\d{8,10})")
    match = patron.search(texto)

    if match:
        return match.group(1)

    return None


# =========================
# 📄 Extracción RUT (texto embebido)
# =========================
def extract_text_pymupdf(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    parts = []
    for page in doc:
        parts.append(page.get_text("text"))
    return "\n".join(parts).strip()


def extract_rut_fields_raw(client: OpenAI, text: str) -> str:
    prompt = f"""
Extrae del siguiente texto (RUT DIAN) ÚNICAMENTE estos campos y devuelve SOLO JSON válido:
- tipo_documento
- numero_identificacion
- primer_apellido
- segundo_apellido
- primer_nombre
- otros_nombres

Reglas:
- Si un campo no aparece, pon null.
- No inventes datos.
- numero_identificacion debe quedar solo con dígitos (sin espacios ni puntos).
- Devuelve únicamente el JSON, sin explicación, sin markdown.

TEXTO:
{text}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Devuelve SOLO JSON válido. Sin markdown."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return resp.choices[0].message.content


def normalizar_campos_rut(data: dict, rut_texto: str = "") -> dict:
    """
    Normaliza salida del LLM + aplica validación anti-error para numero_identificacion (campo 26).
    """
    data = data or {}

    # 1) Primero intenta corregir con regla (campo 26)
    data = corregir_numero_identificacion(data, rut_texto)

    # 2) Luego valida que realmente venga del campo 26
    data["numero_identificacion"] = validar_numero_identificacion(
        rut_texto,
        data.get("numero_identificacion")
    )

    # Normalización de strings
    for k in ["primer_apellido", "segundo_apellido", "primer_nombre", "otros_nombres", "tipo_documento"]:
        data[k] = normalize_text(data.get(k))

    return data


# =========================
# 🪪 Extracción Cédula (PDF imagen -> OCR)
# =========================
def pdf_to_images_pymupdf(pdf_bytes: bytes, zoom: float = 2.5) -> list[Image.Image]:
    """
    Renderiza páginas PDF a imágenes (sin poppler).
    zoom 2.5 = más nitidez para OCR.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images = []
    mat = fitz.Matrix(zoom, zoom)
    for page in doc:
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        images.append(img)
    return images


def ocr_images_easyocr(images: list[Image.Image]) -> str:
    reader = get_ocr_reader()
    all_lines = []
    for img in images:
        img_np = np.array(img)
        lines = reader.readtext(img_np, detail=0)
        all_lines.extend(lines)
    return "\n".join([l for l in all_lines if l and str(l).strip()]).strip()


def extract_cc_fields_raw(client: OpenAI, ocr_text: str) -> str:
    prompt = f"""
A partir del texto OCR de una CÉDULA DE CIUDADANÍA de Colombia, extrae SOLO estos campos y devuelve SOLO JSON válido:
- doc_pais_emisor
- doc_tipo_documento
- doc_numero
- doc_apellidos
- doc_nombres
- doc_fecha_nacimiento
- doc_lugar_nacimiento
- doc_sexo
- doc_estatura
- doc_grupo_sanguineo_rh
- doc_fecha_expedicion
- doc_lugar_expedicion
- doc_registrador
- doc_codigo_barras
- doc_huella_indice
- doc_firma_titular

Reglas:
- Si un campo no aparece, pon null.
- No inventes datos.
- doc_numero debe quedar solo con dígitos (sin puntos ni espacios).
- doc_estatura en metros (ej: 1.57) si aparece.
- doc_huella_indice y doc_firma_titular deben ser "Sí" o "No" si puedes inferirlo por palabras como INDICE/HUELLA/FIRMA.
- Devuelve únicamente JSON, sin explicación, sin markdown.

TEXTO_OCR:
{ocr_text}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Devuelve SOLO JSON válido. Sin markdown."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return resp.choices[0].message.content


def normalizar_campos_cc(data: dict) -> dict:
    data = data or {}
    data["doc_pais_emisor"] = normalize_text(data.get("doc_pais_emisor"))
    data["doc_tipo_documento"] = normalize_text(data.get("doc_tipo_documento"))
    data["doc_numero"] = only_digits(data.get("doc_numero"))
    data["doc_apellidos"] = normalize_text(data.get("doc_apellidos"))
    data["doc_nombres"] = normalize_text(data.get("doc_nombres"))
    data["doc_fecha_nacimiento"] = normalize_date(data.get("doc_fecha_nacimiento"))
    data["doc_lugar_nacimiento"] = normalize_text(data.get("doc_lugar_nacimiento"))
    data["doc_sexo"] = normalize_text(data.get("doc_sexo"))
    data["doc_estatura"] = normalize_text(data.get("doc_estatura"))
    data["doc_grupo_sanguineo_rh"] = normalize_text(data.get("doc_grupo_sanguineo_rh"))
    data["doc_fecha_expedicion"] = normalize_date(data.get("doc_fecha_expedicion"))
    data["doc_lugar_expedicion"] = normalize_text(data.get("doc_lugar_expedicion"))
    data["doc_registrador"] = normalize_text(data.get("doc_registrador"))
    data["doc_codigo_barras"] = normalize_text(data.get("doc_codigo_barras"))

    def norm_si_no(v):
        if v is None:
            return None
        v = str(v).strip().lower()
        if v in ["si", "sí", "s", "yes", "true", "1"]:
            return "Sí"
        if v in ["no", "n", "false", "0"]:
            return "No"
        return None

    data["doc_huella_indice"] = norm_si_no(data.get("doc_huella_indice"))
    data["doc_firma_titular"] = norm_si_no(data.get("doc_firma_titular"))
    return data


# =========================
# 🏦 DOC16 - Certificación bancaria (texto/OCR + IA + reglas)
# =========================
def normalize_date_es(x: str | None) -> str | None:
    """Intenta normalizar fechas en español a YYYY-MM-DD cuando sea posible."""
    if not x:
        return None
    s = str(x).strip()
    s_norm = unicodedata.normalize("NFKC", s).upper()

    # ISO ya
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s_norm):
        return s_norm

    # dd/mm/yyyy o dd-mm-yyyy
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s_norm)
    if m:
        dd = int(m.group(1)); mm = int(m.group(2)); yyyy = int(m.group(3))
        if yyyy < 100:
            yyyy += 2000
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    months = {
        "ENERO":1, "FEBRERO":2, "MARZO":3, "ABRIL":4, "MAYO":5, "JUNIO":6,
        "JULIO":7, "AGOSTO":8, "SEPTIEMBRE":9, "SETIEMBRE":9, "OCTUBRE":10,
        "NOVIEMBRE":11, "DICIEMBRE":12
    }
    # "5 de febrero de 2026" / "05 FEBRERO 2026"
    m = re.search(r"(\d{1,2})\s*(?:DE\s*)?([A-ZÁÉÍÓÚÑ]+)\s*(?:DE\s*)?(\d{4})", s_norm)
    if m and m.group(2) in months:
        dd = int(m.group(1)); mm = months[m.group(2)]; yyyy = int(m.group(3))
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    return s.strip()


def extraer_banco_nit_regla(texto: str) -> str | None:
    # NIT 800.244.627-7 / N.I.T. 800.244.627
    m = re.search(r"\bN\.?I\.?T\.?\s*[:\- ]*([0-9\.]{5,15}(?:\-[0-9])?)", texto, re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def extraer_numero_cuenta_regla(texto: str) -> str | None:
    # Cuenta / No. Cuenta / Cuenta de Inversión
    patterns = [
        r"\bN[°o]?\.?\s*CUENTA\s*[:\- ]*([0-9\- ]{6,30})",
        r"\bCUENTA\s*[:\- ]*([0-9\- ]{6,30})",
        r"\bCUENTA\s+DE\s+INVERSI[ÓO]N\s*[:\- ]*([0-9\- ]{6,30})",
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            cand = re.sub(r"\D", "", m.group(1))
            if 6 <= len(cand) <= 30:
                return cand
    return None


def extraer_estado_cuenta_regla(texto: str) -> str | None:
    t = texto.upper()
    if "INACT" in t:
        return "INACTIVA"
    if "ACTIV" in t:
        return "ACTIVA"
    return None


def extract_doc16_fields_raw(client: OpenAI, text: str) -> str:
    prompt = f"""
A partir del texto de una CERTIFICACIÓN BANCARIA (Colombia), extrae SOLO estos campos y devuelve SOLO JSON válido:
- doc_tipo
- banco_nombre
- banco_nit
- producto_tipo
- producto_nombre
- numero_cuenta
- fecha_apertura
- titular_nombre
- titular_tipo_documento
- titular_num_documento
- estado_cuenta
- fecha_expedicion
- ciudad_expedicion

REGLAS:
- Si un campo no aparece, pon null.
- No inventes datos.
- numero_cuenta y titular_num_documento deben quedar SOLO con dígitos (sin puntos ni espacios) si aplica.
- doc_tipo: si el documento es certificación bancaria escribe "Certificación bancaria"; si no, pon null.
- Devuelve SOLO JSON válido, sin texto adicional.

TEXTO:
{text}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Devuelve SOLO JSON válido. Sin markdown."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return resp.choices[0].message.content


def normalizar_campos_doc16(data: dict, texto: str = "") -> dict:
    data = data or {}

    # Normalizar texto base
    for k in ["doc_tipo","banco_nombre","producto_tipo","producto_nombre",
              "titular_nombre","titular_tipo_documento","ciudad_expedicion"]:
        data[k] = normalize_text(data.get(k))

    # IDs / números
    data["numero_cuenta"] = only_digits(data.get("numero_cuenta"))
    data["titular_num_documento"] = only_digits(data.get("titular_num_documento"))

    # Fechas
    data["fecha_apertura"] = normalize_date_es(data.get("fecha_apertura"))
    data["fecha_expedicion"] = normalize_date_es(data.get("fecha_expedicion"))

    # Reglas complementarias (si IA dejó vacío o raro)
    if not data.get("banco_nit"):
        nit = extraer_banco_nit_regla(texto)
        if nit:
            data["banco_nit"] = nit

    if not data.get("numero_cuenta"):
        nc = extraer_numero_cuenta_regla(texto)
        if nc:
            data["numero_cuenta"] = nc

    if not data.get("estado_cuenta"):
        est = extraer_estado_cuenta_regla(texto)
        if est:
            data["estado_cuenta"] = est

    # Banco nombre: fallback simple por marcas conocidas (evita inventar)
    if not data.get("banco_nombre"):
        t = texto.upper()
        if "BANCOLOMBIA" in t:
            data["banco_nombre"] = "Bancolombia"
        elif "DAVIVIENDA" in t:
            data["banco_nombre"] = "Davivienda"
        elif "SCOTIABANK" in t or "COLPATRIA" in t:
            data["banco_nombre"] = "Scotiabank Colpatria"

    return data


def extract_doc16_text(pdf_bytes: bytes) -> str:
    """Primero intenta texto embebido; si no, OCR a imagen con EasyOCR."""
    text = extract_text_pymupdf(pdf_bytes)
    text = limpiar_texto_para_llm(text)
    if len(text) >= 120:
        return text

    # OCR fallback (1-2 páginas típicamente)
    images = pdf_to_images_pymupdf(pdf_bytes, zoom=2.5)
    ocr_text = ocr_images_easyocr(images)
    return limpiar_texto_para_llm(ocr_text)


# =========================
# ✅ Validaciones
# =========================
def validar_rut_vs_cedula(data_rut: dict, data_cc: dict, logs: list):
    rut_id = only_digits(data_rut.get("numero_identificacion"))
    cc_id = only_digits(data_cc.get("doc_numero"))

    if not rut_id:
        agregar_log(logs, "RUT", "WARNING", "El RUT no tiene número de identificación extraído.")
        return logs

    if not cc_id:
        agregar_log(logs, "CEDULA", "WARNING", "La cédula no tiene número de identificación extraído.")
        return logs

    if rut_id != cc_id:
        agregar_log(
            logs,
            "VALIDACION_CRUZADA",
            "WARNING",
            f"El número de identificación del RUT ({rut_id}) NO coincide con el de la cédula ({cc_id})."
        )
    else:
        agregar_log(logs, "VALIDACION_CRUZADA", "INFO", "El número de identificación del RUT coincide con el de la cédula.")

    return logs


def validar_cedula_vacia(data_cc: dict, logs: list):
    cc_id = only_digits(data_cc.get("doc_numero"))
    if not cc_id:
        agregar_log(logs, "CEDULA", "WARNING", "El campo doc_numero de la cédula está vacío o no fue detectado correctamente.")
    return logs


def validar_fecha_certificacion_bancaria(data_doc16: dict, logs: list):
    fecha_str = data_doc16.get("fecha_expedicion")

    if not fecha_str:
        agregar_log(logs, "CERTIFICACION_BANCARIA", "WARNING", "No se encontró fecha de expedición en la certificación bancaria.")
        return logs

    try:
        fecha_doc = datetime.strptime(fecha_str, "%Y-%m-%d")
        hoy = datetime.today()

        diferencia_dias = (hoy - fecha_doc).days

        if diferencia_dias > 30:
            agregar_log(
                logs,
                "CERTIFICACION_BANCARIA",
                "WARNING",
                f"La certificación bancaria tiene {diferencia_dias} días de expedición (mayor a 30 días)."
            )
        else:
            agregar_log(
                logs,
                "CERTIFICACION_BANCARIA",
                "INFO",
                f"La certificación bancaria fue expedida hace {diferencia_dias} días (vigente)."
            )

    except Exception as e:
        agregar_log(logs, "CERTIFICACION_BANCARIA", "ERROR", f"Error al procesar fecha de expedición: {str(e)}")

    return logs


# =========================
# 📦 Diccionario maestro + Excel consolidado
# =========================
MASTER_ROWS = [
    # ---------- DOC14 (RUT) ----------
    {"doc_id": "DOC14", "Fuente": "DOC14_RUT_DIAN", "Caracterización variable": "Identificación personal",
     "Nombre de la Variable": "tipo_documento", "Tipo_Variable": "texto", "Caracterización": "Tipo documento"},
    {"doc_id": "DOC14", "Fuente": "DOC14_RUT_DIAN", "Caracterización variable": "Identificación personal",
     "Nombre de la Variable": "numero_identificacion", "Tipo_Variable": "texto", "Caracterización": "Número de identificación"},
    {"doc_id": "DOC14", "Fuente": "DOC14_RUT_DIAN", "Caracterización variable": "Identificación personal",
     "Nombre de la Variable": "primer_apellido", "Tipo_Variable": "texto", "Caracterización": "Primer apellido"},
    {"doc_id": "DOC14", "Fuente": "DOC14_RUT_DIAN", "Caracterización variable": "Identificación personal",
     "Nombre de la Variable": "segundo_apellido", "Tipo_Variable": "texto", "Caracterización": "Segundo apellido"},
    {"doc_id": "DOC14", "Fuente": "DOC14_RUT_DIAN", "Caracterización variable": "Identificación personal",
     "Nombre de la Variable": "primer_nombre", "Tipo_Variable": "texto", "Caracterización": "Primer nombre"},
    {"doc_id": "DOC14", "Fuente": "DOC14_RUT_DIAN", "Caracterización variable": "Identificación personal",
     "Nombre de la Variable": "otros_nombres", "Tipo_Variable": "texto", "Caracterización": "Otros nombres"},

    # ---------- DOC12 (Cédula) ----------
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Identificación del documento",
     "Nombre de la Variable": "doc_tipo", "Tipo_Variable": "texto", "Caracterización": "Tipo (diccionario)"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_pais_emisor", "Tipo_Variable": "texto", "Caracterización": "País emisor"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_tipo_documento", "Tipo_Variable": "texto", "Caracterización": "Tipo documento"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_numero", "Tipo_Variable": "texto", "Caracterización": "Número"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_apellidos", "Tipo_Variable": "texto", "Caracterización": "Apellidos"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_nombres", "Tipo_Variable": "texto", "Caracterización": "Nombres"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_fecha_nacimiento", "Tipo_Variable": "fecha", "Caracterización": "Fecha de nacimiento"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_lugar_nacimiento", "Tipo_Variable": "texto", "Caracterización": "Lugar de nacimiento"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_sexo", "Tipo_Variable": "texto", "Caracterización": "Sexo"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_estatura", "Tipo_Variable": "texto", "Caracterización": "Estatura"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_grupo_sanguineo_rh", "Tipo_Variable": "texto", "Caracterización": "Grupo sanguíneo y RH"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_fecha_expedicion", "Tipo_Variable": "fecha", "Caracterización": "Fecha de expedición"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_lugar_expedicion", "Tipo_Variable": "texto", "Caracterización": "Lugar de expedición"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_registrador", "Tipo_Variable": "texto", "Caracterización": "Registrador"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Datos del documento",
     "Nombre de la Variable": "doc_codigo_barras", "Tipo_Variable": "texto", "Caracterización": "Código de barras"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Biométricos (opcional)",
     "Nombre de la Variable": "doc_huella_indice", "Tipo_Variable": "texto", "Caracterización": "Huella índice"},
    {"doc_id": "DOC12", "Fuente": "DOC12_DocumentoIdentificacion", "Caracterización variable": "Firma (opcional)",
     "Nombre de la Variable": "doc_firma_titular", "Tipo_Variable": "texto", "Caracterización": "Firma titular"},

    # ---------- DOC16 (Certificación bancaria) ----------
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Identificación del documento",
     "Nombre de la Variable": "doc_tipo", "Tipo_Variable": "texto", "Caracterización": "Certificación bancaria / Certifica a quien interese que…"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Entidad financiera",
     "Nombre de la Variable": "banco_nombre", "Tipo_Variable": "texto", "Caracterización": "Nombre banco"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Entidad financiera",
     "Nombre de la Variable": "banco_nit", "Tipo_Variable": "texto", "Caracterización": "NIT banco"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Producto",
     "Nombre de la Variable": "producto_tipo", "Tipo_Variable": "texto", "Caracterización": "Tipo producto (Cuenta de ahorro / corriente)"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Producto",
     "Nombre de la Variable": "producto_nombre", "Tipo_Variable": "texto", "Caracterización": "Nombre del producto"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Producto",
     "Nombre de la Variable": "numero_cuenta", "Tipo_Variable": "texto", "Caracterización": "Número de cuenta"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Producto",
     "Nombre de la Variable": "fecha_apertura", "Tipo_Variable": "fecha", "Caracterización": "Fecha de apertura"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Titular",
     "Nombre de la Variable": "titular_nombre", "Tipo_Variable": "texto", "Caracterización": "Nombre del titular"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Titular",
     "Nombre de la Variable": "titular_tipo_documento", "Tipo_Variable": "texto", "Caracterización": "Tipo documento titular"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Titular",
     "Nombre de la Variable": "titular_num_documento", "Tipo_Variable": "texto", "Caracterización": "Número documento titular"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Estado",
     "Nombre de la Variable": "estado_cuenta", "Tipo_Variable": "texto", "Caracterización": "Estado (ACTIVA/INACTIVA)"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Expedición",
     "Nombre de la Variable": "fecha_expedicion", "Tipo_Variable": "texto", "Caracterización": "Fecha de expedición (día/mes/año en texto)"},
    {"doc_id": "DOC16", "Fuente": "DOC16_CertificacionBancaria", "Caracterización variable": "Expedición",
     "Nombre de la Variable": "ciudad_expedicion", "Tipo_Variable": "texto", "Caracterización": "Ciudad (si está indicada)"},
]


# =========================
# 📊 Métricas mínimas (tiempo / completitud / warnings)
# =========================
def campos_esperados_por_doc(doc_id: str) -> list[str]:
    """Devuelve los campos esperados según el diccionario maestro."""
    return [r["Nombre de la Variable"] for r in MASTER_ROWS if r["doc_id"] == doc_id]

def calcular_completitud(data: dict | None, campos_esperados: list[str]) -> float | None:
    """% de campos esperados con valor no vacío (None/'' se considera vacío)."""
    if not data or not campos_esperados:
        return None
    llenos = 0
    for k in campos_esperados:
        v = data.get(k)
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        llenos += 1
    return round(100 * llenos / len(campos_esperados), 1)

def contar_warnings(logs: list, documento: str) -> int:
    return sum(1 for l in (logs or []) if l.get("tipo") == "WARNING" and l.get("documento") == documento)


def fill_master_values(rut_data: dict | None, cc_data: dict | None, doc16_data: dict | None) -> pd.DataFrame:
    rows = [r.copy() for r in MASTER_ROWS]

    # RUT
    if rut_data:
        for r in rows:
            if r["doc_id"] == "DOC14":
                key = r["Nombre de la Variable"]
                r["Valor"] = rut_data.get(key)

    # Cédula
    if cc_data:
        cc_data = cc_data.copy()
        cc_data.setdefault("doc_pais_emisor", "República de Colombia")
        cc_data.setdefault("doc_tipo_documento", "Cédula de ciudadanía")

        for r in rows:
            if r["doc_id"] == "DOC12":
                key = r["Nombre de la Variable"]
                if key == "doc_tipo":
                    r["Valor"] = "Documento de identidad (Cédula de ciudadanía) – imagen anverso/reverso"
                else:
                    r["Valor"] = cc_data.get(key)

    # DOC16 - Certificación bancaria
    if doc16_data:
        for r in rows:
            if r["doc_id"] == "DOC16":
                key = r["Nombre de la Variable"]
                if key == "doc_tipo":
                    # Valor fijo del diccionario (evita que el LLM invente)
                    r["Valor"] = "Certificación bancaria"
                else:
                    r["Valor"] = doc16_data.get(key)

    return pd.DataFrame(rows)


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="extraccion")
    return output.getvalue()


# =========================
# 🔎 Clasificación mínima por nombre (para N archivos)
# =========================
def guess_doc_id_by_filename(filename: str) -> Optional[str]:
    """
    MVP: clasifica por nombre para DOC14/DOC12/DOC16.
    Para los 28, aquí irás sumando reglas o migras a clasificador IA.
    """
    f = (filename or "").upper()
    if "RUT" in f or "DOC14" in f:
        return "DOC14"
    if "CED" in f or "CEDULA" in f or "DOC12" in f or "DOCUMENTOIDENTIFICACION" in f or "DOCU" in f:
        return "DOC12"
    if "CERTIFICACION" in f or "BANCARIA" in f or "DOC16" in f:
        return "DOC16"
    return None


# =========================
# ✅ Pipeline principal (sin UI) — equivalente a tu botón "Procesar todo"
# =========================
def run_pipeline(items: List[DocItem], client: OpenAI) -> Dict[str, Any]:
    """
    Procesa N documentos (por ahora implementa DOC14/DOC12/DOC16).
    Retorna:
      - rut_data, cc_data, doc16_data
      - df_master (records)
      - excel_bytes
      - logs
      - metricas (tiempo/completitud/warnings)
    """
    # Inicializaciones (evita NameError)
    rut_data = None
    cc_data = None
    doc16_data = None
    rut_texto = ""
    doc16_texto = ""

    logs = inicializar_logs()

    # Métricas mínimas
    metricas = {
        "tiempo_por_documento_s": {},
        "completitud_por_documento_pct": {},
        "warnings_por_documento": {},
    }

    # 1) Clasificar items: map doc_id -> lista (por si llegan duplicados)
    buckets: Dict[str, List[DocItem]] = {"DOC14": [], "DOC12": [], "DOC16": [], "UNKNOWN": []}
    for it in items:
        doc_id = guess_doc_id_by_filename(it.original_name)
        if not doc_id:
            buckets["UNKNOWN"].append(it)
        else:
            buckets[doc_id].append(it)

    # 2) DOC14 (RUT) — si llegan varios, tomamos el primero (MVP)
    if buckets["DOC14"]:
        it = buckets["DOC14"][0]
        t0_rut = time.perf_counter()
        rut_bytes = open(it.path, "rb").read()

        rut_texto = extract_text_pymupdf(rut_bytes)
        rut_texto = limpiar_texto_para_llm(rut_texto)
        if len(rut_texto) < 100:
            # En tu app solo advertías; acá dejamos el texto vacío y la IA extrae lo que pueda
            rut_texto = ""

        raw = extract_rut_fields_raw(client, rut_texto)
        rut_data = normalizar_campos_rut(safe_json_loads(raw), rut_texto=rut_texto)

        # Fallback OCR SOLO para numero_identificacion (campo 26)
        id_ocr = None
        rut_num = rut_data.get("numero_identificacion")

        if numero_id_es_sospechoso(rut_num):
            id_ocr = ocr_numero_identificacion_desde_campo26(rut_bytes)
            if id_ocr:
                rut_data["numero_identificacion"] = id_ocr
                rut_data["_fuente_numero_identificacion"] = "ocr_campo26"

        if not id_ocr:
            numero_validado = validar_numero_identificacion(rut_texto, rut_data.get("numero_identificacion"))
            if numero_validado:
                rut_data["numero_identificacion"] = numero_validado
                rut_data["_fuente_numero_identificacion"] = "validado_campo26"
            else:
                rut_data["_fuente_numero_identificacion"] = "ia_no_validado"

        metricas["tiempo_por_documento_s"]["RUT"] = round(time.perf_counter() - t0_rut, 3)
        metricas["completitud_por_documento_pct"]["RUT"] = calcular_completitud(
            rut_data, campos_esperados_por_doc("DOC14")
        )
    else:
        metricas["tiempo_por_documento_s"]["RUT"] = None
        metricas["completitud_por_documento_pct"]["RUT"] = None

    # 3) DOC12 (Cédula) — si llegan varios, tomamos el primero (MVP)
    if buckets["DOC12"]:
        it = buckets["DOC12"][0]
        t0_cc = time.perf_counter()
        cc_bytes = open(it.path, "rb").read()

        images = pdf_to_images_pymupdf(cc_bytes, zoom=2.5)
        cc_ocr_text = ocr_images_easyocr(images)
        cc_ocr_text = limpiar_texto_para_llm(cc_ocr_text)

        raw_cc = extract_cc_fields_raw(client, cc_ocr_text)
        cc_data = normalizar_campos_cc(safe_json_loads(raw_cc))

        # Métricas Cédula (con valores fijos del diccionario)
        cc_eval = (cc_data or {}).copy()
        cc_eval["doc_tipo"] = "Documento de identidad (Cédula de ciudadanía) – imagen anverso/reverso"
        cc_eval.setdefault("doc_pais_emisor", "República de Colombia")
        cc_eval.setdefault("doc_tipo_documento", "Cédula de ciudadanía")

        metricas["tiempo_por_documento_s"]["CEDULA"] = round(time.perf_counter() - t0_cc, 3)
        metricas["completitud_por_documento_pct"]["CEDULA"] = calcular_completitud(
            cc_eval, campos_esperados_por_doc("DOC12")
        )
    else:
        metricas["tiempo_por_documento_s"]["CEDULA"] = None
        metricas["completitud_por_documento_pct"]["CEDULA"] = None

    # 4) DOC16 (Certificación bancaria) — si llegan varios, tomamos el primero (MVP)
    if buckets["DOC16"]:
        it = buckets["DOC16"][0]
        t0_doc16 = time.perf_counter()
        doc16_bytes = open(it.path, "rb").read()

        doc16_texto = extract_doc16_text(doc16_bytes)

        raw_16 = extract_doc16_fields_raw(client, doc16_texto)
        doc16_data = normalizar_campos_doc16(safe_json_loads(raw_16), texto=doc16_texto)

        # Métricas DOC16 (con valor fijo doc_tipo)
        doc16_eval = (doc16_data or {}).copy()
        doc16_eval["doc_tipo"] = "Certificación bancaria"

        metricas["tiempo_por_documento_s"]["DOC16"] = round(time.perf_counter() - t0_doc16, 3)
        metricas["completitud_por_documento_pct"]["DOC16"] = calcular_completitud(
            doc16_eval, campos_esperados_por_doc("DOC16")
        )
    else:
        metricas["tiempo_por_documento_s"]["DOC16"] = None
        metricas["completitud_por_documento_pct"]["DOC16"] = None

    # 5) Validación (NO forzar) + logs
    if cc_data:
        logs = validar_cedula_vacia(cc_data, logs)

    if doc16_data:
        logs = validar_fecha_certificacion_bancaria(doc16_data, logs)

    if rut_data and cc_data:
        logs = validar_rut_vs_cedula(rut_data, cc_data, logs)

    # Warnings por documento (según tu lógica)
    metricas["warnings_por_documento"]["RUT"] = contar_warnings(logs, "RUT")
    metricas["warnings_por_documento"]["CEDULA"] = contar_warnings(logs, "CEDULA")
    metricas["warnings_por_documento"]["DOC16"] = contar_warnings(logs, "CERTIFICACION_BANCARIA")
    metricas["warnings_por_documento"]["VALIDACION_CRUZADA"] = contar_warnings(logs, "VALIDACION_CRUZADA")

    # 6) Consolidado + Excel
    df_master = fill_master_values(rut_data, cc_data, doc16_data)
    excel_bytes = dataframe_to_excel_bytes(df_master)

    return {
        "rut_data": rut_data,
        "cc_data": cc_data,
        "doc16_data": doc16_data,
        "df_master": df_master.to_dict(orient="records"),
        "excel_bytes": excel_bytes,
        "logs": logs,
        "metricas": metricas,
        "uploads_resumen": {
            "DOC14": [x.original_name for x in buckets["DOC14"]],
            "DOC12": [x.original_name for x in buckets["DOC12"]],
            "DOC16": [x.original_name for x in buckets["DOC16"]],
            "UNKNOWN": [x.original_name for x in buckets["UNKNOWN"]],
        }
    }


# =========================
# Helper opcional
# =========================
def build_openai_client_from_env() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Falta OPENAI_API_KEY en variables de entorno del backend.")
    return OpenAI(api_key=api_key)
