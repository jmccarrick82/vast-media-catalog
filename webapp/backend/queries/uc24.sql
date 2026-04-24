-- UC24: Syndication Revenue Tracking
-- Joins assets with syndication_records to show per-territory
-- and per-licensee distribution details.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.syndication_licensee_count,
    a.syndication_territory_count,
    sr.record_id,
    sr.licensee,
    sr.territory,
    sr.delivery_version_id,
    sr.license_status,
    sr.tracked_at
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".syndication_records sr
    ON a.asset_id = sr.asset_id
ORDER BY sr.tracked_at DESC, a.asset_id
LIMIT {{LIMIT}}
