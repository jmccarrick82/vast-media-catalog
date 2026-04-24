"""S3 client for downloading video files from VAST S3-compatible storage."""

import os
import tempfile
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig


class S3Client:
    """Client for accessing video files on VAST via S3 protocol."""

    def __init__(self, config: dict):
        self.endpoint = config["s3"]["endpoint"]
        self.access_key = config["s3"]["access_key"]
        self.secret_key = config["s3"]["secret_key"]
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=BotoConfig(signature_version="s3v4"),
                region_name="us-east-1",
            )
        return self._client

    @staticmethod
    def parse_s3_path(s3_path: str) -> tuple[str, str]:
        """Parse s3://bucket/key into (bucket, key)."""
        parsed = urlparse(s3_path)
        if parsed.scheme != "s3":
            raise ValueError(f"Expected s3:// URL, got: {s3_path}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    def download_to_temp(self, s3_path: str) -> str:
        """Download an S3 object to a temporary file. Returns the temp file path.

        Caller is responsible for cleaning up the temp file.
        """
        bucket, key = self.parse_s3_path(s3_path)
        ext = os.path.splitext(key)[1] or ".mp4"
        tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
        tmp_path = tmp.name
        tmp.close()

        print(f"  Downloading s3://{bucket}/{key} -> {tmp_path}")
        self.client.download_file(bucket, key, tmp_path)
        return tmp_path

    def get_object_metadata(self, s3_path: str) -> dict:
        """Get S3 object metadata (size, last modified, etc.)."""
        bucket, key = self.parse_s3_path(s3_path)
        response = self.client.head_object(Bucket=bucket, Key=key)
        return {
            "content_length": response["ContentLength"],
            "last_modified": response["LastModified"].isoformat(),
            "content_type": response.get("ContentType", ""),
            "etag": response.get("ETag", "").strip('"'),
        }

    def file_exists(self, s3_path: str) -> bool:
        """Check if an S3 object exists."""
        try:
            self.get_object_metadata(s3_path)
            return True
        except Exception:
            return False

    def upload_file(self, local_path: str, s3_path: str, metadata: dict = None):
        """Upload a local file to S3.

        Args:
            local_path: Path to the local file to upload.
            s3_path: Destination s3://bucket/key path.
            metadata: Optional dict of S3 user-defined metadata to attach.
        """
        bucket, key = self.parse_s3_path(s3_path)
        extra_args = {}
        if metadata:
            extra_args["Metadata"] = {str(k): str(v) for k, v in metadata.items()}

        print(f"  Uploading {local_path} -> s3://{bucket}/{key}")
        self.client.upload_file(
            local_path, bucket, key,
            ExtraArgs=extra_args if extra_args else None,
        )
