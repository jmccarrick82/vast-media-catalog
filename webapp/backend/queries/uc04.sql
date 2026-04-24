-- UC04: License Audit Trail
-- Shows assets with license audit data including licensor,
-- usage type, and derivative counts for compliance tracking.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.licensor,
    a.license_type,
    a.usage_type,
    a.audit_derivative_count,
    a.territories,
    a.rights_expiry,
    a.license_audit_at
FROM vast."{{SCHEMA}}".assets a
WHERE a.license_audit_at IS NOT NULL
ORDER BY a.license_audit_at DESC
LIMIT {{LIMIT}}
