-- UC10: Version Control Across the Lifecycle
-- Joins assets with version_history to show version chains,
-- tracking how content evolves through revisions.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.version_number,
    a.version_label,
    vh.version_id,
    vh.version_number AS history_version_number,
    vh.previous_version_id,
    vh.version_label AS history_version_label,
    vh.created_at AS version_created_at,
    p.filename AS previous_version_filename,
    p.s3_path AS previous_version_path
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".version_history vh
    ON a.asset_id = vh.asset_id
LEFT JOIN vast."{{SCHEMA}}".assets p
    ON vh.previous_version_id = p.asset_id
ORDER BY a.asset_id, vh.version_number
LIMIT {{LIMIT}}
