import os
import requests
import streamlit as st

st.set_page_config(page_title="OCR Atenea (Frontend)", layout="wide")

BACKEND_URL = st.secrets.get("BACKEND_URL", os.getenv("BACKEND_URL", "http://localhost:8000"))

st.title("📄 OCR Atenea — Frontend (Streamlit)")
st.caption("Sube documentos (hasta 28 o más), procesa en backend y descarga Excel.")

with st.sidebar:
    st.subheader("⚙️ Configuración")
    st.write("Backend URL:")
    st.code(BACKEND_URL)
    st.info("En enterprise, la OpenAI API key vive solo en el backend (Secrets).")

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

        up = requests.post(f"{BACKEND_URL}/upload", files=multi, timeout=300)
        if up.status_code != 200:
            st.error(f"Error en /upload: {up.status_code} - {up.text}")
            st.stop()

        case_id = up.json()["case_id"]
        st.success(f"✅ Upload listo. case_id: {case_id}")

    with st.spinner("Procesando en backend (OCR + extracción + validaciones)..."):
        pr = requests.post(f"{BACKEND_URL}/process/{case_id}", timeout=1200)
        if pr.status_code != 200:
            st.error(f"Error en /process: {pr.status_code} - {pr.text}")
            st.stop()

    st.success("✅ Procesamiento completo")

    with st.spinner("Cargando resultados..."):
        rr = requests.get(f"{BACKEND_URL}/results/{case_id}", timeout=300)
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
