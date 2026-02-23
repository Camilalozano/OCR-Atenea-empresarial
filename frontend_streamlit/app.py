import os

import requests
import streamlit as st

st.set_page_config(page_title="OCR Atenea (Frontend)", layout="wide")


def _first_non_empty(values: list[str | None]) -> str:
    for value in values:
        if value and value.strip():
            return value
    return ""


def _read_secret(name: str) -> str | None:
    try:
        value = st.secrets.get(name)
        if isinstance(value, str):
            return value
    except FileNotFoundError:
        return None
    return None


def _resolve_default_backend_url() -> str:
    candidate_names = ["BACKEND_URL", "BACKEND_API_URL", "API_BASE_URL"]
    from_secrets = [_read_secret(name) for name in candidate_names]
    from_env = [os.getenv(name) for name in candidate_names]

    configured = _first_non_empty(from_secrets + from_env)
    return configured or "http://localhost:8000"



def _clean_backend_url(raw_url: str) -> str:
    url = raw_url.strip().rstrip("/")
    if url and "://" not in url:
        url = f"https://{url}"
    return url


def _is_localhost_url(url: str) -> bool:
    low = url.lower()
    return "localhost" in low or "127.0.0.1" in low


DEFAULT_BACKEND_URL = _clean_backend_url(_resolve_default_backend_url())

if "backend_url" not in st.session_state:
    st.session_state.backend_url = DEFAULT_BACKEND_URL

st.title("📄 OCR Atenea — Frontend (Streamlit)")
st.caption("Sube documentos (hasta 28 o más), procesa en backend y descarga Excel.")

with st.sidebar:
    st.subheader("⚙️ Configuración")
    st.text_input(
        "Backend URL",
        key="backend_url",
        placeholder="https://mi-backend.onrender.com",
        help="URL pública del backend (sin localhost si estás en Streamlit Cloud).",
    )
    BACKEND_URL = _clean_backend_url(st.session_state.backend_url)

    if BACKEND_URL:
        st.caption(f"Backend actual: `{BACKEND_URL}`")

    st.info("En enterprise, la OpenAI API key vive solo en el backend (Secrets).")
    if _is_localhost_url(BACKEND_URL):
        st.warning(
            "Usando backend local (`localhost`). Esto funciona cuando ejecutas frontend+backend en tu máquina. "
            "Si este frontend está desplegado (Streamlit Cloud), cambia a la URL pública de tu backend."
        )

st.subheader("1) Cargar documentos")
files = st.file_uploader(
    "Sube tus documentos (PDF/Imagen). Puedes cargar muchos a la vez.",
    type=["pdf", "png", "jpg", "jpeg"],
    accept_multiple_files=True,
)

colA, colB = st.columns(2)
with colA:
    do_process = st.button(
        "🚀 Subir y procesar",
        type="primary",
        disabled=(not files or not BACKEND_URL),
        help="Carga al menos un archivo y configura un Backend URL para habilitar este botón.",
    )
with colB:
    st.write("")

if do_process and files:
    if not BACKEND_URL:
        st.error("`Backend URL` es obligatorio. Ingresa la URL de tu backend para continuar.")
        st.stop()

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
