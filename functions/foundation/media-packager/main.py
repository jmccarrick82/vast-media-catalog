"""james-media-packager — Phase 3 of the pre-ingest pipeline.

Trigger: S3 PUT on `s3://james-media-clips/<source_id>/_ready.json`
(the marker ai-clipper drops when it finishes). Other PUTs in that bucket
are ignored — we only fire on the sentinel.

Pipeline per event:
  1. Parse the marker → (source_id, clip_ids).
  2. Load source_videos row + all extracted_clips rows for that source.
  3. Build licensing (uploader S3 metadata overrides config defaults).
  4. For each clip:
      a. Download the raw clip from james-media-clips.
      b. Extract one JPEG thumbnail.
      c. For each configured rendition preset:
           i.  Transcode with ffmpeg
           ii. Sign with c2patool → embedded C2PA manifest in the MP4
          iii. Upload to s3://<deliveries_bucket>/<package_id>/clips/<n>/<preset>.mp4
          iv.  Write package_renditions row
      d. Upload thumbnail.
  5. Assemble the JSON sidecar manifest + upload to
     s3://<deliveries_bucket>/<package_id>/manifest.json
  6. Write/finalize delivery_packages row.

All thresholds + presets + C2PA config live in function_configs scopes
`packager` and `provenance`.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import traceback
import uuid

from config_loader import load_config as load_json_config
from config import load_config
from s3_client import S3Client

from ingest import ffprobe, transcode, thumbnail, manifest, provenance
from ingest import s3_helpers, tables
from schemas import DELIVERY_PACKAGES_SCHEMA, PACKAGE_RENDITIONS_SCHEMA  # noqa: F401


_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("FFMPEG_BINARY",   os.path.join(_BIN_DIR, "ffmpeg"))
os.environ.setdefault("FFPROBE_BINARY",  os.path.join(_BIN_DIR, "ffprobe"))
os.environ.setdefault("C2PATOOL_BINARY", os.path.join(_BIN_DIR, "c2patool"))


def _fix_cert_perms():
    """Knative sometimes flips private-key perms to 0600 during the pod
    build, and c2patool runs as a non-root user that can't read it.
    Make the key world-readable at handler-init time so signing just works.
    Dev cert on a self-signed chain — we're not protecting it as a secret.
    """
    for name in ("signing.key", "signing.pub"):
        p = os.path.join(_BIN_DIR, "c2pa-signing", name)
        try:
            if os.path.isfile(p):
                os.chmod(p, 0o644)
        except Exception:
            pass


_fix_cert_perms()


def init(ctx):
    cfg_file = load_json_config()
    s3 = S3Client(cfg_file)

    ctx.user_data = {
        "cfg_file":           cfg_file,
        "s3":                 s3,
        "bucket":             cfg_file["vast"]["bucket"],
        "schema":             cfg_file["vast"].get("schema", "media-catalog"),
        "clips_bucket":       cfg_file.get("buckets", {}).get("clips_bucket", "james-media-clips"),
        "deliveries_bucket":  cfg_file.get("buckets", {}).get("deliveries_bucket", "james-media-deliveries"),
        "catalog_bucket":     cfg_file.get("buckets", {}).get("catalog_bucket", "james-media-catalog"),
    }

    try:
        import vastdb
        session = vastdb.connect(
            endpoint=cfg_file["vast"]["endpoint"],
            access=cfg_file["vast"]["access_key"],
            secret=cfg_file["vast"]["secret_key"],
        )
        tables.ensure_delivery_packages_table(session,
                                              ctx.user_data["bucket"],
                                              ctx.user_data["schema"])
        tables.ensure_package_renditions_table(session,
                                               ctx.user_data["bucket"],
                                               ctx.user_data["schema"])
        ctx.logger.info("delivery_packages + package_renditions tables ready")
    except Exception as e:
        ctx.logger.info(f"WARN: table setup deferred: {e}")

    # Log c2pa readiness and config snapshots
    try:
        prov_cfg = load_config("provenance")
        pkg_cfg  = load_config("packager")
        ctx.logger.info(f"provenance config keys: {list(prov_cfg.snapshot().keys())}")
        ctx.logger.info(f"packager config keys:   {list(pkg_cfg.snapshot().keys())}")
    except Exception as e:
        ctx.logger.info(f"WARN: config load: {e}")

    ctx.logger.info("media-packager initialized")


def handler(ctx, event):
    log = ctx.logger.info
    t_start = time.monotonic()
    try:
        return _handle(ctx, event, log, t_start)
    except Exception as e:
        log(f"HANDLER ERROR: {type(e).__name__}: {e}")
        log(f"TRACEBACK: {traceback.format_exc()}")
        raise


def _handle(ctx, event, log, t_start):
    s3_path = _parse_s3_event(event)
    _, key = s3_helpers.parse_s3_path(s3_path)

    # Skip anything that isn't a ready-marker. The trigger fires on every
    # PUT to the clips bucket — most of those are clip MP4s uploaded by
    # ai-clipper, which we don't want to re-process here.
    if not key.endswith("/_ready.json"):
        log(f"[skip] not a ready marker: {s3_path}")
        return json.dumps({"skipped": True, "reason": "not a _ready.json marker"})

    log(f"[1/6] packager triggered on {s3_path}")

    # ── 1. Read the marker ────────────────────────────────────────────
    try:
        body = ctx.user_data["s3"].client.get_object(
            Bucket=ctx.user_data["clips_bucket"],
            Key=key,
        )["Body"].read()
        marker = json.loads(body.decode("utf-8"))
    except Exception as e:
        log(f"       failed to read marker: {e}")
        raise

    source_id = marker["source_id"]
    filename  = marker.get("filename") or "unknown"
    log(f"       source_id={source_id}  clips={marker.get('clip_count')}")

    # ── 2. Load DB rows ───────────────────────────────────────────────
    import vastdb, ibis
    cfg_file = ctx.user_data["cfg_file"]
    session = vastdb.connect(
        endpoint=cfg_file["vast"]["endpoint"],
        access=cfg_file["vast"]["access_key"],
        secret=cfg_file["vast"]["secret_key"],
    )
    bucket, schema = ctx.user_data["bucket"], ctx.user_data["schema"]

    with session.transaction() as tx:
        src_tbl = tx.bucket(bucket).schema(schema).table("source_videos")
        t = ibis.table(src_tbl.columns())
        src_rows = src_tbl.select(predicate=t.source_id == source_id).read_all().to_pylist()
        if not src_rows:
            raise RuntimeError(f"no source_videos row for {source_id}")
        source_row = src_rows[0]

        clips_tbl = tx.bucket(bucket).schema(schema).table("extracted_clips")
        ct = ibis.table(clips_tbl.columns())
        clip_rows = clips_tbl.select(
            predicate=(ct.source_id == source_id) & (ct.status == "active")
        ).read_all().to_pylist()
    clip_rows.sort(key=lambda c: c.get("clip_index") or 0)

    if not clip_rows:
        log("       no active clips for source; nothing to package")
        return json.dumps({"source_id": source_id, "clips": 0,
                           "reason": "no active clips"})

    log(f"[2/6] loaded {len(clip_rows)} clips; source={filename}")

    # ── 3. Config snapshots ───────────────────────────────────────────
    pkg_cfg  = load_config("packager")
    prov_cfg = load_config("provenance")
    pkg_snap  = pkg_cfg.snapshot()
    prov_snap = prov_cfg.snapshot()
    presets = pkg_snap.get("rendition_presets") or []
    source_h = source_row.get("height")
    presets = [p for p in presets if transcode.should_emit_preset(p, source_h)]
    log(f"[3/6] {len(presets)} renditions × {len(clip_rows)} clips "
        f"= {len(presets) * len(clip_rows)} outputs; c2pa={prov_snap.get('c2pa_enabled')}")

    # ── 4. Mint package + seed row ────────────────────────────────────
    package_id = uuid.uuid4().hex[:12]
    pkg_root_s3 = f"s3://{ctx.user_data['deliveries_bucket']}/{package_id}"
    pkg_created_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Licensing: uploader S3 metadata overrides config defaults
    try:
        src_meta = s3_helpers.get_object_tags(
            ctx.user_data["s3"].client,
            source_row.get("current_s3_path") or source_row.get("s3_inbox_path"),
        )
    except Exception:
        src_meta = {}

    lic_defaults = {
        "rights_cleared_for":  pkg_snap.get("default_rights_cleared_for") or [],
        "restrictions":        pkg_snap.get("default_restrictions") or [],
        "clearance_days":      int(pkg_snap.get("default_clearance_days") or 90),
        "source_attribution":  (pkg_snap.get("source_attribution_template") or
                                "VAST Media Catalog / source: {filename}").format(
            filename=filename, source_id=source_id,
        ),
        "notes":               pkg_snap.get("default_licensing_notes") or "",
    }
    licensing = manifest.normalize_licensing(src_meta, lic_defaults, time.time())

    tables.upsert_delivery_package(session, bucket, schema, {
        "package_id":           package_id,
        "source_id":            source_id,
        "package_root_s3_path": pkg_root_s3,
        "rights_cleared_for_json": tables.json_runs(licensing["rights_cleared_for"]),
        "restrictions_json":       tables.json_runs(licensing["restrictions"]),
        "source_attribution":      licensing["source_attribution"],
        "clearance_expires_at":    licensing["clearance_expires_at"],
        "licensing_notes":         licensing["notes"],
        "c2pa_enabled":            bool(prov_snap.get("c2pa_enabled")),
        "c2pa_claim_generator":    prov_snap.get("claim_generator"),
        "status":                  "building",
    })

    # ── 5. Process each clip ──────────────────────────────────────────
    work_dir = os.path.join("/tmp", f"pkg-{package_id}")
    os.makedirs(work_dir, exist_ok=True)

    renditions_by_clip: dict = {}
    thumbnails_by_clip: dict = {}
    c2pa_signed_count = 0
    c2pa_errors: list = []
    total_size = 0

    try:
        for clip in clip_rows:
            clip_id = clip["clip_id"]
            clip_idx = clip.get("clip_index") or 0
            log(f"[4/6] clip {clip_idx:03d} ({clip_id[:8]}...)")

            # Download raw clip
            src_clip_s3 = clip["clip_s3_path"]
            local_src = os.path.join(work_dir, f"raw-{clip_idx:03d}.mp4")
            try:
                b, k = s3_helpers.parse_s3_path(src_clip_s3)
                ctx.user_data["s3"].client.download_file(b, k, local_src)
            except Exception as e:
                log(f"       download failed: {e}"); continue

            # Thumbnail
            thumb_rel = None
            if pkg_snap.get("thumbnail_enabled"):
                thumb_local = os.path.join(work_dir, f"thumb-{clip_idx:03d}.jpg")
                try:
                    thumbnail.extract_thumbnail(
                        local_src, thumb_local,
                        duration=clip.get("duration_seconds"),
                        max_width=int(pkg_snap.get("thumbnail_max_width") or 1280),
                        quality=int(pkg_snap.get("thumbnail_quality") or 4),
                    )
                    thumb_key = f"{package_id}/thumbnails/clip-{clip_idx:03d}.jpg"
                    thumb_s3  = f"s3://{ctx.user_data['deliveries_bucket']}/{thumb_key}"
                    ctx.user_data["s3"].upload_file(
                        thumb_local, thumb_s3,
                        metadata={"source-id": source_id, "clip-id": clip_id,
                                  "package-id": package_id},
                    )
                    thumb_rel = f"thumbnails/clip-{clip_idx:03d}.jpg"
                    thumbnails_by_clip[clip_id] = thumb_rel
                    log(f"       thumbnail → {thumb_s3}")
                except Exception as e:
                    log(f"       thumbnail failed: {e}")

            # Transcode + sign each preset
            rendition_results: list = []
            for preset in presets:
                pname = preset.get("name") or "rendition"
                local_rend = os.path.join(work_dir, f"clip-{clip_idx:03d}-{pname}.mp4")
                try:
                    rr = transcode.transcode(
                        local_src, local_rend, preset,
                        timeout=float(pkg_snap.get("transcode_timeout_seconds") or 300),
                        threads=int(pkg_snap.get("transcode_threads") or 2),
                    )
                except Exception as e:
                    log(f"       {pname}: transcode failed: {e}")
                    continue

                # Build a single-rendition placeholder of the manifest
                # just-enough for provenance.build_c2pa_claim_for_rendition.
                # We'll rebuild the full manifest after the loop.
                tmp_manifest_for_claim = {
                    "source": {"filename": filename, "source_id": source_id},
                    "licensing": licensing,
                }
                rendition_info = {
                    "name": pname,
                    "container": rr.container,
                    "video_codec": rr.video_codec,
                    "audio_codec": rr.audio_codec,
                    "width": rr.width,
                    "height": rr.height,
                    "video_bitrate": rr.video_bitrate,
                    "audio_bitrate": rr.audio_bitrate,
                    "size_bytes": rr.file_size_bytes,
                }

                # Sign
                signed_local = local_rend + ".signed.mp4"
                c2pa_label = None
                c2pa_signer = None
                c2pa_error  = None
                if prov_snap.get("c2pa_enabled"):
                    try:
                        sr = provenance.sign_rendition(
                            src_path=local_rend,
                            out_path=signed_local,
                            manifest=tmp_manifest_for_claim,
                            clip=clip,
                            rendition=rendition_info,
                            cfg_snapshot=prov_snap,
                        )
                        c2pa_label  = sr.manifest_label
                        c2pa_signer = sr.signer
                        # Replace the local file with the signed one so
                        # the upload below picks up the signature.
                        os.replace(signed_local, local_rend)
                        c2pa_signed_count += 1
                        log(f"       {pname}: signed ({c2pa_label[:24]}...)")
                    except Exception as e:
                        c2pa_error = str(e)[:300]
                        c2pa_errors.append({"clip_id": clip_id, "rendition": pname,
                                            "error": c2pa_error})
                        log(f"       {pname}: C2PA sign failed: {c2pa_error}")

                # Upload to deliveries bucket
                rend_key = f"{package_id}/clips/clip-{clip_idx:03d}/{pname}.mp4"
                rend_s3  = f"s3://{ctx.user_data['deliveries_bucket']}/{rend_key}"
                try:
                    ctx.user_data["s3"].upload_file(
                        local_rend, rend_s3,
                        metadata={"source-id":  source_id,
                                  "clip-id":    clip_id,
                                  "package-id": package_id,
                                  "rendition":  pname},
                    )
                except Exception as e:
                    log(f"       {pname}: upload failed: {e}")
                    continue

                size = os.path.getsize(local_rend) if os.path.isfile(local_rend) else 0
                total_size += size

                # sha256 for the rendition (post-sign) — cheap and useful
                sha = _sha256_file(local_rend)

                rendition_id = hashlib.md5(rend_s3.encode()).hexdigest()
                tables.upsert_package_rendition(session, bucket, schema, {
                    "rendition_id":     rendition_id,
                    "package_id":       package_id,
                    "clip_id":          clip_id,
                    "source_id":        source_id,
                    "rendition_name":   pname,
                    "container":        rr.container,
                    "video_codec":      rr.video_codec,
                    "audio_codec":      rr.audio_codec,
                    "width":            rr.width,
                    "height":           rr.height,
                    "video_bitrate":    rr.video_bitrate,
                    "audio_bitrate":    rr.audio_bitrate,
                    "rendition_s3_path": rend_s3,
                    "file_size_bytes":  size,
                    "sha256":           sha,
                    "c2pa_signed":      bool(c2pa_label),
                    "c2pa_manifest_label": c2pa_label,
                    "c2pa_signer":      c2pa_signer,
                    "c2pa_signed_at":   time.time() if c2pa_label else None,
                    "c2pa_error":       c2pa_error,
                    "status":           "ready",
                })
                rendition_results.append({
                    **rendition_info,
                    "relative_path":    f"clips/clip-{clip_idx:03d}/{pname}.mp4",
                    "file_size_bytes":  size,
                    "c2pa_signed":      bool(c2pa_label),
                    "c2pa_manifest_label": c2pa_label,
                    "rendition_id":     rendition_id,
                })

            renditions_by_clip[clip_id] = rendition_results

        # ── 6. Assemble + upload sidecar manifest ──────────────────
        log("[5/6] writing sidecar manifest...")
        full_manifest = manifest.build_package_manifest(
            package_id=package_id,
            source_row=source_row,
            clip_rows=clip_rows,
            renditions_by_clip=renditions_by_clip,
            thumbnails_by_clip=thumbnails_by_clip,
            licensing=licensing,
            created_at_iso=pkg_created_iso,
            claim_generator=prov_snap.get("claim_generator") or "vast-media-catalog",
            c2pa_enabled=bool(prov_snap.get("c2pa_enabled")),
        )
        manifest_s3 = None
        if pkg_snap.get("sidecar_manifest_enabled"):
            manifest_s3 = f"s3://{ctx.user_data['deliveries_bucket']}/{package_id}/manifest.json"
            s3_helpers.put_bytes(
                ctx.user_data["s3"].client, manifest_s3,
                manifest.to_sidecar_json(full_manifest).encode(),
                content_type="application/json",
                metadata={"source-id": source_id, "package-id": package_id},
            )
            log(f"       manifest → {manifest_s3}")

        # ── 7. Finalize delivery_packages row ───────────────────────
        rendition_count = sum(len(v) for v in renditions_by_clip.values())
        tables.upsert_delivery_package(session, bucket, schema, {
            "package_id":            package_id,
            "manifest_s3_path":      manifest_s3,
            "clip_count":            len(clip_rows),
            "rendition_count":       rendition_count,
            "total_size_bytes":      total_size,
            "c2pa_signed_count":     c2pa_signed_count,
            "c2pa_signer":           prov_snap.get("claim_generator")
                                       if c2pa_signed_count else None,
            "c2pa_errors_json":      tables.json_runs(c2pa_errors) if c2pa_errors else None,
            "status":                "ready" if rendition_count > 0 else "failed",
            "status_reason":         ("signed={} errors={}".format(
                                         c2pa_signed_count, len(c2pa_errors))),
        })

        # ── 8. Stamp source_videos as packaged ──────────────────────
        tables.upsert_source_video(session, bucket, schema, {
            "source_id":        source_id,
            "packaging_status": "done",
            "package_id":       package_id,
            "packaged_at":      time.time(),
        })

    finally:
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)

    total = time.monotonic() - t_start
    log(f"Done. package {package_id} — {len(clip_rows)} clips × "
        f"{len(presets)} renditions = {sum(len(v) for v in renditions_by_clip.values())} outputs "
        f"(c2pa signed: {c2pa_signed_count}) in {total:.2f}s")
    return json.dumps({
        "package_id":         package_id,
        "source_id":          source_id,
        "clip_count":         len(clip_rows),
        "rendition_count":    sum(len(v) for v in renditions_by_clip.values()),
        "c2pa_signed_count":  c2pa_signed_count,
        "manifest_s3_path":   manifest_s3,
        "elapsed":            total,
    })


# ── helpers ────────────────────────────────────────────────────────────

def _parse_s3_event(event) -> str:
    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        return f"s3://{event.bucket}/{event.object_key}"
    if hasattr(event, "body"):
        b = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        return b.strip()
    raise RuntimeError(f"unsupported event type: {type(event).__name__}")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
