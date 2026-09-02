"""
FastAPI app entrypoint.

Run with: uvicorn backend.main:app --reload --port 8000

No CORS middleware: Streamlit calls this API from its own Python process (server-side `requests` calls), not from browser JS 
CORS only applies to browser-originated cross-origin requests, so it's a non-issue here.
"""
from fastapi import FastAPI

from backend.routes import documents, status

app = FastAPI(title="Political Speeches Analytics — Collection API")

app.include_router(documents.router, tags=["documents"])
app.include_router(status.router, tags=["status"])


@app.get("/health")
def health():
    return {"status": "ok"}