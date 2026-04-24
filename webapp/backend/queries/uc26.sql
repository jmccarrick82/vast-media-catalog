-- UC26: Co-Production Attribution
-- Joins assets with production_entities to show per-company
-- ownership splits and contribution types.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.primary_production_company,
    pe.attribution_id,
    pe.production_company,
    pe.crew_origin,
    pe.ownership_split_pct,
    pe.contribution_type,
    pe.attributed_at
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".production_entities pe
    ON a.asset_id = pe.asset_id
ORDER BY a.asset_id, pe.ownership_split_pct DESC
LIMIT {{LIMIT}}
