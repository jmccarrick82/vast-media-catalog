-- UC16: Clearance Inheritance
-- Shows assets with clearance status and their inheritance chain,
-- tracking how clearance propagates through derivative works.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.clearance_status,
    a.clearance_type,
    a.clearance_inherited_from,
    a.clearance_recorded_at,
    p.filename AS inherited_from_filename,
    p.s3_path AS inherited_from_path,
    p.clearance_status AS parent_clearance_status
FROM vast."{{SCHEMA}}".assets a
LEFT JOIN vast."{{SCHEMA}}".assets p
    ON a.clearance_inherited_from = p.asset_id
WHERE a.clearance_status IS NOT NULL
ORDER BY a.clearance_recorded_at DESC
LIMIT {{LIMIT}}
