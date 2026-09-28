"""Uploads: the browser cannot hand the worker a local path, so files go to the MinIO `staging` bucket first."""
import mimetypes
import os
import re
import uuid

from fastapi import APIRouter, HTTPException, UploadFile

from backend import storage
from backend.schemas import UploadResponse

router = APIRouter()

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(5 * 1024 ** 3)))


def safe_name(name: str) -> str:
    base = os.path.basename((name or "").replace("\\", "/"))
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._")[:120]
    return cleaned or "upload"


@router.post("/uploads", response_model=UploadResponse, status_code=201)
def upload_file(file: UploadFile):
    name = safe_name(file.filename)
    file.file.seek(0, os.SEEK_END)
    size = file.file.tell()
    file.file.seek(0)
    if size == 0:
        raise HTTPException(status_code=422, detail="The uploaded file is empty.")
    if size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large (limit {MAX_UPLOAD_BYTES} bytes).")
    content_type = file.content_type or mimetypes.guess_type(name)[0]
    key = f"{uuid.uuid4()}/{name}"
    try:
        storage.put_staging(key, file.file, size, content_type)
    except storage.StorageUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return UploadResponse(staging_key=key, file_name=name, size_bytes=size, content_type=content_type)
