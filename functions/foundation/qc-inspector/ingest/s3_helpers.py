"""Small S3 utilities callers share across pre-ingest functions.

All helpers take an explicit boto3 client so the caller controls the
session/config. This keeps the primitives pure and testable with a
stub client.
"""

from typing import Optional


def parse_s3_path(s3_path: str):
    """'s3://bucket/key/with/slashes.mp4' → ('bucket', 'key/with/slashes.mp4')."""
    if not s3_path or not s3_path.startswith("s3://"):
        raise ValueError(f"not an s3:// path: {s3_path!r}")
    rest = s3_path[len("s3://"):]
    if "/" not in rest:
        raise ValueError(f"s3:// path missing key: {s3_path!r}")
    bucket, key = rest.split("/", 1)
    return bucket, key


def copy_object(s3_client, src_s3: str, dst_s3: str,
                metadata: Optional[dict] = None) -> dict:
    """Server-side copy. `metadata` (if provided) becomes x-amz-meta-*
    tags on the destination object, replacing anything on the source.

    Returns the CopyObject response.
    """
    src_bucket, src_key = parse_s3_path(src_s3)
    dst_bucket, dst_key = parse_s3_path(dst_s3)
    kwargs = {
        "Bucket":     dst_bucket,
        "Key":        dst_key,
        "CopySource": {"Bucket": src_bucket, "Key": src_key},
    }
    if metadata:
        kwargs["Metadata"] = {k: str(v) for k, v in metadata.items()}
        kwargs["MetadataDirective"] = "REPLACE"
    return s3_client.copy_object(**kwargs)


def delete_object(s3_client, s3_path: str) -> dict:
    bucket, key = parse_s3_path(s3_path)
    return s3_client.delete_object(Bucket=bucket, Key=key)


def move_object(s3_client, src_s3: str, dst_bucket: str,
                new_key: Optional[str] = None,
                metadata: Optional[dict] = None,
                preserve_metadata: bool = True) -> str:
    """Copy-and-delete: move an object to a different bucket.

    If `preserve_metadata=True` (default), reads the source object's
    x-amz-meta-* tags first and MERGES them with the `metadata` arg —
    caller's tags win on key collision. This means uploader-provided
    tags like `clip-prompt` survive the move.

    If `new_key` is None, the original key is reused. Returns the
    destination s3 path.
    """
    src_bucket, src_key = parse_s3_path(src_s3)
    key = new_key or src_key
    dst_s3 = f"s3://{dst_bucket}/{key}"

    final_meta: Optional[dict] = None
    if preserve_metadata:
        try:
            existing = get_object_tags(s3_client, src_s3)
        except Exception:
            existing = {}
        final_meta = dict(existing)
        if metadata:
            final_meta.update(metadata)
    else:
        final_meta = metadata

    copy_object(s3_client, src_s3, dst_s3, metadata=final_meta)
    delete_object(s3_client, src_s3)
    return dst_s3


def download_to_temp(s3_client, s3_path: str, suffix: str = "") -> str:
    """Download to a temp file and return the local path. Caller owns cleanup."""
    import tempfile
    bucket, key = parse_s3_path(s3_path)
    f = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    f.close()
    s3_client.download_file(bucket, key, f.name)
    return f.name


def get_object_tags(s3_client, s3_path: str) -> dict:
    """Return x-amz-meta-* metadata as a plain dict (keys lowercased without prefix)."""
    bucket, key = parse_s3_path(s3_path)
    resp = s3_client.head_object(Bucket=bucket, Key=key)
    return resp.get("Metadata") or {}
