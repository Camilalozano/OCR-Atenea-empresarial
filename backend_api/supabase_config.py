import os
from supabase import create_client

# (Opcional pero recomendado si usas backend_api/.env en local)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
BUCKET = (os.getenv("SUPABASE_BUCKET") or "ocr-atenea").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Faltan variables de entorno de Supabase. "
        "Asegura SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY en tu .env o en el servidor."
    )

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_bytes(path: str, content: bytes, content_type: str = "application/octet-stream"):
    """
    path ejemplo:
    {case_id}/input/RUT.pdf
    {case_id}/output/result.xlsx
    """
    return supabase.storage.from_(BUCKET).upload(
        path=path,
        file=content,
        file_options={"content-type": content_type, "upsert": "true"},
    )
