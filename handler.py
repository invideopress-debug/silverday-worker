"""Runpod Serverless Worker (DEMO)

Этот воркер НЕ использует реальную видео-модель. Он генерирует короткое видео через ffmpeg (однотонный фон),
загружает его в S3 и дергает ваш webhook.

Замените функцию generate_video() на реальную инференс-логику (Kling/Seedance/свой pipeline).
"""

import os
import json
import subprocess
import tempfile
from typing import Any

import boto3
import requests
import runpod


def s3_client(s3_cfg: dict):
    return boto3.client(
        "s3",
        region_name=s3_cfg.get("region", "us-east-1"),
        endpoint_url=s3_cfg.get("endpoint_url"),
        aws_access_key_id=s3_cfg.get("access_key"),
        aws_secret_access_key=s3_cfg.get("secret_key"),
    )


def generate_video(out_path: str, *, duration_s: int, fps: int, width: int, height: int):
    # ffmpeg -f lavfi -i color=c=black:s=1280x720:r=24 -t 5 out.mp4
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c=black:s={width}x{height}:r={fps}",
        "-t",
        str(duration_s),
        "-pix_fmt",
        "yuv420p",
        out_path,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def call_webhook(url: str, secret: str, payload: dict[str, Any]):
    headers = {}
    if secret:
        headers["X-Webhook-Secret"] = secret
    requests.post(url, json=payload, headers=headers, timeout=20)


def handler(event):
    inp = event.get("input") or {}

    job_id = inp.get("job_id")
    params = (inp.get("params") or {})
    s3_cfg = inp.get("s3") or {}
    bucket = s3_cfg.get("bucket")
    output_prefix = inp.get("output_prefix")

    webhook = inp.get("webhook") or {}
    webhook_url = webhook.get("url")
    webhook_secret = webhook.get("secret", "")

    if not all([job_id, bucket, output_prefix, webhook_url]):
        return {"ok": False, "error": "missing required fields"}

    # Stage: running
    try:
        call_webhook(webhook_url, webhook_secret, {"job_id": job_id, "status": "running"})
    except Exception:
        pass

    duration_s = int(params.get("duration_s", 5))
    fps = int(params.get("fps", 24))
    width = int(params.get("width", 1280))
    height = int(params.get("height", 720))

    s3 = s3_client(s3_cfg)

    try:
        with tempfile.TemporaryDirectory() as td:
            out_video = os.path.join(td, "out.mp4")
            generate_video(out_video, duration_s=duration_s, fps=fps, width=width, height=height)

            # Stage: uploading
            try:
                call_webhook(webhook_url, webhook_secret, {"job_id": job_id, "status": "uploading"})
            except Exception:
                pass

            output_key = f"{output_prefix}/video.mp4"
            s3.upload_file(out_video, bucket, output_key, ExtraArgs={"ContentType": "video/mp4"})

        # Done
        call_webhook(
            webhook_url,
            webhook_secret,
            {
                "job_id": job_id,
                "status": "done",
                "output_s3_key": output_key,
                "preview_s3_key": None,
            },
        )

        return {"ok": True, "output_s3_key": output_key}

    except Exception as e:
        # Fail
        try:
            call_webhook(webhook_url, webhook_secret, {"job_id": job_id, "status": "failed", "error": str(e)})
        except Exception:
            pass
        return {"ok": False, "error": str(e)}


runpod.serverless.start({"handler": handler})
