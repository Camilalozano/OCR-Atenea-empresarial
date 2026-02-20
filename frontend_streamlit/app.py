import os
import requests
import streamlit as st

st.set_page_config(page_title="OCR Atenea (Frontend)", layout="wide")

try:
    default_backend_from_secrets = st.secrets.get("BACKEND_URL")
except FileNotFoundError:
    default_backend_from_secrets = None

DEFAULT_BACKEND_URL = default_backend_from_secrets or os.getenv("BACKEND_URL", "http://localhost:8000")


def _clean_backend_url(raw_url: str) -> str:
    return raw_url.strip().rstrip("/")

st.title("📄 OCR Atenea — Frontend (Streamlit)")
st.caption("Sube documentos (hasta 28 o más), procesa en backend y descarga Excel.")

with st.sidebar:
    st.subheader("⚙️ Configuración")
    backend_url_input = st.text_input("Backend URL", value=DEFAULT_BACKEND_URL)
    BACKEND_URL = _clean_backend_url(backend_url_input)
    st.write("Backend URL:")
    st.code(BACKEND_URL)
    st.info("En enterprise, la OpenAI API key vive solo en el backend (Secrets).")
    if "localhost" in BACKEND_URL or "127.0.0.1" in BACKEND_URL:
        st.warning(
            "Si este frontend está desplegado (Streamlit Cloud), `localhost` no apunta a tu backend remoto. "
            "Configura aquí la URL pública del backend (ej: https://mi-backend.onrender.com)."
        )

st.subheader("1) Cargar documentos")
files = st.file_uploader(
    "Sube tus documentos (PDF/Imagen). Puedes cargar muchos a la vez.",
    type=["pdf", "png", "jpg", "jpeg"],
    accept_multiple_files=True
)

colA, colB = st.columns(2)
with colA:
    do_process = st.button("🚀 Subir y procesar", type="primary", disabled=(not files))
with colB:
    st.write("")

if do_process and files:
    with st.spinner("Subiendo archivos al backend..."):
        multi = []
        for f in files:
            # content-type aproximado
            ct = "application/pdf" if f.name.lower().endswith(".pdf") else "image/jpeg"
            multi.append(("files", (f.name, f.getvalue(), ct)))

        try:
            up = requests.post(f"{BACKEND_URL}/upload", files=multi, timeout=300)
        except requests.exceptions.RequestException as exc:
            st.error(
                "No se pudo conectar con el backend. "
                "Revisa que `Backend URL` sea accesible públicamente y que el backend esté encendido."
            )
            st.exception(exc)
            st.stop()

        if up.status_code != 200:
            st.error(f"Error en /upload: {up.status_code} - {up.text}")
            st.stop()

        case_id = up.json()["case_id"]
        st.success(f"✅ Upload listo. case_id: {case_id}")

    with st.spinner("Procesando en backend (OCR + extracción + validaciones)..."):
        try:
            pr = requests.post(f"{BACKEND_URL}/process/{case_id}", timeout=1200)
        except requests.exceptions.RequestException as exc:
            st.error("Fallo de conexión en /process. Revisa backend URL y estado del backend.")
            st.exception(exc)
            st.stop()

        if pr.status_code != 200:
            st.error(f"Error en /process: {pr.status_code} - {pr.text}")
            st.stop()

    st.success("✅ Procesamiento completo")

    with st.spinner("Cargando resultados..."):
        try:
            rr = requests.get(f"{BACKEND_URL}/results/{case_id}", timeout=300)
        except requests.exceptions.RequestException as exc:
            st.error("Fallo de conexión en /results. Revisa backend URL y estado del backend.")
            st.exception(exc)
            st.stop()

        if rr.status_code != 200:
            st.error(f"Error en /results: {rr.status_code} - {rr.text}")
            st.stop()

        payload = rr.json()
        result = payload.get("result", {})
        metricas = result.get("metricas", {})
        logs = result.get("logs", {}).get("items", [])
        df_master = result.get("df_master", [])

    st.subheader("2) Métricas")
    st.json(metricas)

    st.subheader("3) Tabla master (preview)")
    if df_master:
        st.dataframe(df_master, use_container_width=True)
    else:
        st.info("No hay filas en df_master (aún).")

    st.subheader("4) Logs")
    if logs:
        st.dataframe(logs, use_container_width=True)
    else:
        st.info("Sin logs.")

    st.subheader("5) Descargar Excel")
    excel_url = f"{BACKEND_URL}/export/{case_id}"
    st.markdown(f"➡️ Descarga desde: {excel_url}")
    # Si quieres, puedes bajar el Excel en memoria y usar st.download_button.
