import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET = os.getenv("SUPABASE_BUCKET", "ocr-atenea")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_bytes(path: str, content: bytes, content_type: str = "application/octet-stream"):
    """
    path ejemplo:
    {case_id}/input/RUT.pdf
    {case_id}/output/result.xlsx
    """
    res = supabase.storage.from_(BUCKET).upload(
        path=path,
        file=content,
        file_options={"content-type": content_type, "upsert": "true"},
    )
    return res

