
import os
import json
import logging
import boto3
import tempfile
from botocore.exceptions import ClientError
from gradio_client import Client

# Global clients and configuration
s3_client = None
vlm_api_url = None
vlm_model_name = None
ingestion_prompt = None

def init(ctx):
    """
    Initializes the function context, setting up the S3 client and VLM configuration.
    """
    global s3_client, vlm_api_url, vlm_model_name, ingestion_prompt

    # The VAST runtime provides a pre-configured logger.
    ctx.logger.info("Initializing function...")

    # S3 Client Initialization
    try:
        s3_endpoint = os.environ.get("S3_ENDPOINT")
        s3_access_key = os.environ.get("S3_ACCESS_KEY")
        s3_secret_key = os.environ.get("S3_SECRET_KEY")

        if not all([s3_access_key, s3_secret_key]):
            ctx.logger.warning("S3 credentials not fully provided. Using anonymous access if possible.")

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        )
        ctx.logger.info("S3 client initialized successfully.")
        if s3_endpoint:
            ctx.logger.info(f"Using S3 endpoint: {s3_endpoint}")

    except Exception as e:
        ctx.logger.error(f"Failed to initialize S3 client: {e}", exc_info=True)
        raise

    # VLM Configuration
    vlm_api_url = os.environ.get("VLM_API_ENDPOINT")
    vlm_model_name = os.environ.get("VLM_MODEL_NAME", "qwen-vl-plus")
    ingestion_prompt = os.environ.get("INGESTION_PROMPT", "Summarize this video.")

    if not vlm_api_url:
        ctx.logger.error("VLM_API_ENDPOINT environment variable is not set.")
        raise ValueError("VLM API endpoint is required.")

    ctx.logger.info(f"Gradio VLM endpoint configured: {vlm_api_url}")
    ctx.logger.info("Function initialization complete.")

def handler(ctx, event):
    """
    Processes incoming CloudEvents to summarize videos using the Gradio client.
    """
    ctx.logger.info(f"Received event with ID: {event.id}")
    ctx.logger.debug(f"Full event details: {event}")

    s3_bucket, s3_key = get_file_location(ctx, event)
    if not s3_bucket or not s3_key:
        ctx.logger.error("Could not determine file location from the event.")
        return {"status": "error", "message": "Missing file location in event data."}

    if not s3_key.lower().endswith(".mp4"):
        ctx.logger.info(f"Skipping file as it is not a .mp4 video: {s3_key}")
        return {"status": "skipped", "message": "File is not a .mp4 video."}

    ctx.logger.info(f"Processing video file: s3://{s3_bucket}/{s3_key}")

    try:
        video_content = download_from_s3(ctx, s3_bucket, s3_key)
        if not video_content:
            return {"status": "error", "message": "Failed to download video."}

        summary = summarize_video(ctx, video_content)
        if not summary:
            return {"status": "error", "message": "Failed to get summary from VLM."}

        output_data = {
            "ingestion_prompt": ingestion_prompt,
            "model_details": {
                "model_name": vlm_model_name,
                "api_endpoint": vlm_api_url,
            },
            "video_summary": summary,
            "source_file": f"s3://{s3_bucket}/{s3_key}",
        }
        ctx.logger.info("Successfully generated video summary.")

        output_key = os.path.splitext(s3_key)[0] + ".json"
        upload_to_s3(ctx, s3_bucket, output_key, json.dumps(output_data, indent=2))

        ctx.logger.info(f"Successfully uploaded summary to s3://{s3_bucket}/{output_key}")
        return {"status": "success", "output_location": f"s3://{s3_bucket}/{output_key}"}

    except Exception as e:
        ctx.logger.error(f"An unexpected error occurred during processing: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

def get_file_location(ctx, event):
    """Extracts S3 bucket and key from a VAST CloudEvent."""
    if event.type == "Element":
        try:
            element_event = event.as_element_event()
            bucket = element_event.bucket
            key = element_event.object_key
            ctx.logger.info(f"Extracted from Element event: s3://{bucket}/{key}")
            return bucket, key
        except (TypeError, AttributeError) as e:
            ctx.logger.warning(f"Could not parse as Element event, falling back to data payload. Error: {e}")

    event_data = event.get_data()
    bucket = event_data.get("s3_bucket")
    key = event_data.get("s3_key")
    if bucket and key:
        ctx.logger.info(f"Extracted from event data payload: s3://{bucket}/{key}")
        return bucket, key

    return None, None

def download_from_s3(ctx, bucket, key):
    """Downloads a file from S3."""
    try:
        ctx.logger.info(f"Downloading s3://{bucket}/{key}...")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read()
        ctx.logger.info(f"Successfully downloaded {len(content)} bytes.")
        return content
    except ClientError as e:
        ctx.logger.error(f"S3 ClientError while downloading: {e}", exc_info=True)
        return None

def summarize_video(ctx, video_content):
    """Calls the Gradio VLM API using the gradio_client library with the corrected payload."""
    try:
        ctx.logger.info(f"Connecting to Gradio client at {vlm_api_url}...")
        client = Client(vlm_api_url)

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=True) as temp_video_file:
            temp_video_file.write(video_content)
            temp_video_file.flush()
            ctx.logger.info(f"Video content written to temporary file: {temp_video_file.name}")

            # Construct the payload to match the server's Pydantic model, including the 'meta' field.
            # The client library handles the upload and creates the FileData object, but we must
            # structure the input to predict() correctly.
            video_file_data = {
                "path": temp_video_file.name,
                "meta": {"_type": "gradio.FileData"}
            }
            video_param = {"video": video_file_data, "subtitles": None}

            ctx.logger.info("Sending request to Gradio with api_name='/chat'...")
            result = client.predict(
                prompt=ingestion_prompt,
                image_file=None,
                video_file=video_param,
                temperature=0,
                max_tokens=1024,
                video_fps=1.0,
                api_name="/chat"
            )
            ctx.logger.info("Successfully received response from VLM.")

            if isinstance(result, list) and len(result) > 0:
                summary = result[0]
            else:
                summary = result

            return summary

    except Exception as e:
        ctx.logger.error(f"Error calling Gradio VLM API with gradio_client: {e}", exc_info=True)
        return None

def upload_to_s3(ctx, bucket, key, content):
    """Uploads content to a specified S3 key."""
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType="application/json")
        ctx.logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
    except ClientError as e:
        ctx.logger.error(f"S3 ClientError while uploading: {e}", exc_info=True)
        raise
