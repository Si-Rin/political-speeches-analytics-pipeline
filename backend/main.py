"""
FastAPI app entrypoint.

Run with: uvicorn backend.main:app --reload --port 8000

No CORS middleware: Streamlit calls this API from its own Python process (server-side `requests` calls), not from browser JS
CORS only applies to browser-originated cross-origin requests, so it's a non-issue here.
"""
from fastapi import FastAPI

from backend.routes import analytics, documents, runs, status, topics_run, uploads

app = FastAPI(title="Political Speeches Analytics — Collection API")

app.include_router(runs.router, tags=["runs"])
app.include_router(uploads.router, tags=["uploads"])
app.include_router(documents.router, tags=["documents"])
app.include_router(status.router, tags=["status"])
app.include_router(analytics.router, tags=["analytics"])
app.include_router(topics_run.router, tags=["analytics"])


@app.get("/health")
def health():
    return {"status": "ok"}