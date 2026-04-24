-- UC23: Content Valuation
-- Shows assets ordered by commercial value score, with value tier,
-- derivative counts, reuse counts, and delivery metrics.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.commercial_value_score,
    a.value_tier,
    a.valuation_derivative_count,
    a.reuse_count,
    a.delivery_count,
    a.valued_at,
    a.asset_classification
FROM vast."{{SCHEMA}}".assets a
WHERE a.commercial_value_score IS NOT NULL
ORDER BY a.commercial_value_score DESC
LIMIT {{LIMIT}}
