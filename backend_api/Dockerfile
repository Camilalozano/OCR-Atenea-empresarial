FROM python:3.11-slim

WORKDIR /app

# instala dependencias
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# copia el código
COPY . .

# Render expone este puerto
EXPOSE 10000

# arranca FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
