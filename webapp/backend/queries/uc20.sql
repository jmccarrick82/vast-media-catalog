-- UC20: Regulatory Compliance (GDPR / AI Act)
-- Joins assets with gdpr_personal_data to show per-person details
-- of personally identifiable information detected in content.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.gdpr_faces_detected,
    a.gdpr_persons_identified,
    a.gdpr_blast_radius,
    g.detection_id,
    g.person_id,
    g.data_type,
    g.face_detected,
    g.frame_timestamps,
    g.detected_at
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".gdpr_personal_data g
    ON a.asset_id = g.asset_id
ORDER BY a.gdpr_blast_radius DESC, a.asset_id
LIMIT {{LIMIT}}
