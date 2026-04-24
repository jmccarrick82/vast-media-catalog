-- UC21: Chain of Custody for Legal Hold
-- Shows assets under legal hold with integrity verification status,
-- hash-at-hold values, and related asset counts.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.legal_hold_active,
    a.sha256,
    a.sha256_at_hold,
    a.hold_placed_at,
    a.integrity_verified,
    a.related_asset_count,
    a.custody_verified_at,
    CASE
        WHEN a.sha256 = a.sha256_at_hold THEN 'INTACT'
        WHEN a.sha256_at_hold IS NULL THEN 'NO_BASELINE'
        ELSE 'MODIFIED'
    END AS integrity_status
FROM vast."{{SCHEMA}}".assets a
WHERE a.legal_hold_active = true
ORDER BY a.hold_placed_at DESC
LIMIT {{LIMIT}}
