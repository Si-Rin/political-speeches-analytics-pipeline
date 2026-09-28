"""MinIO access for the API: the `staging` bucket that receives user uploads (the flow reads and deletes them)."""
import os
from typing import BinaryIO, Optional

from minio import Minio
from minio.error import S3Error

STAGING_BUCKET = os.environ.get("MINIO_STAGING_BUCKET", "staging")
STAGING_EXPIRY_DAYS = int(os.environ.get("MINIO_STAGING_EXPIRY_DAYS", "1"))


class StorageUnavailable(RuntimeError):
    pass


def get_client() -> Minio:
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def ensure_staging_bucket(client: Minio) -> None:
    """Create the bucket if missing and add an expiry rule as the backstop for uploads nobody ingests."""
    if client.bucket_exists(STAGING_BUCKET):
        return
    client.make_bucket(STAGING_BUCKET)
    try:
        from minio.commonconfig import ENABLED, Filter
        from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule
        client.set_bucket_lifecycle(STAGING_BUCKET, LifecycleConfig([
            Rule(ENABLED, rule_filter=Filter(prefix=""), rule_id="expire-staging",
                 expiration=Expiration(days=STAGING_EXPIRY_DAYS))]))
    except Exception:
        pass    # best effort: the flow deletes staged objects itself


def put_staging(key: str, fileobj: BinaryIO, size: int, content_type: Optional[str], client: Optional[Minio] = None) -> None:
    try:
        client = client or get_client()
        ensure_staging_bucket(client)
        client.put_object(STAGING_BUCKET, key, fileobj, length=size,
                          content_type=content_type or "application/octet-stream")
    except (S3Error, OSError) as exc:
        raise StorageUnavailable(f"Could not store the upload: {exc}") from exc


def staging_exists(key: str, client: Optional[Minio] = None) -> bool:
    try:
        (client or get_client()).stat_object(STAGING_BUCKET, key)
        return True
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchBucket"):
            return False
        raise StorageUnavailable(f"Could not reach object storage: {exc}") from exc
    except OSError as exc:
        raise StorageUnavailable(f"Could not reach object storage: {exc}") from exc
