FROM python:3.11-slim

WORKDIR /app

COPY frontend_requirements.txt .

RUN pip install --no-cache-dir -r frontend_requirements.txt

COPY frontend ./frontend

EXPOSE 8501

CMD ["streamlit", "run", "frontend/app.py", "--server.address=0.0.0.0", "--server.port=8501"]