-- UC01: Rights Conflict Detection
-- Shows assets where rights conflicts have been detected,
-- with license details, territories, restrictions, and conflict specifics.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.license_type,
    a.territories,
    a.restrictions,
    a.rights_expiry,
    a.conflict_detected,
    a.conflict_details,
    a.rights_checked_at
FROM vast."{{SCHEMA}}".assets a
WHERE a.conflict_detected = true
ORDER BY a.rights_checked_at DESC
LIMIT {{LIMIT}}
