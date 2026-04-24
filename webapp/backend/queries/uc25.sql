-- UC25: Insurance & Disaster Recovery Valuation
-- Shows assets with irreplaceability status, digital copy counts,
-- replacement cost tiers, and commercial history scores.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.is_irreplaceable,
    a.has_digital_copies,
    a.digital_copy_count,
    a.replacement_cost_tier,
    a.commercial_history_score,
    a.insurance_valued_at,
    a.file_size_bytes,
    a.value_tier
FROM vast."{{SCHEMA}}".assets a
WHERE a.replacement_cost_tier IS NOT NULL
ORDER BY
    a.is_irreplaceable DESC,
    CASE a.replacement_cost_tier
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
        ELSE 5
    END,
    a.commercial_history_score DESC
LIMIT {{LIMIT}}
