-- UC05: Talent & Music Residuals
-- Joins assets with the talent_music detail table to show
-- face/music detections per asset with timestamps and confidence.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.faces_detected_count,
    a.music_detected,
    tm.detection_id,
    tm.detection_type,
    tm.label,
    tm.confidence,
    tm.start_time_sec,
    tm.end_time_sec,
    tm.detected_at
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".talent_music tm
    ON a.asset_id = tm.asset_id
ORDER BY a.asset_id, tm.start_time_sec
LIMIT {{LIMIT}}
