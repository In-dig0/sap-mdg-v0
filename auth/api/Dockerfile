# POSIZIONE: mdg-v0/auth/api/Dockerfile

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY auth_main.py .

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "auth_main:app", "--host", "0.0.0.0", "--port", "8001"]
