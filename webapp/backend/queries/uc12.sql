-- UC12: Model Contamination Detection
-- Shows assets with contamination risk assessment, indicating
-- whether AI-processed content has polluted the provenance chain.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.contamination_risk,
    a.has_ai_processing_upstream,
    a.processing_depth,
    a.ai_probability,
    a.ai_tool_detected,
    a.training_dataset_id,
    a.contamination_checked_at
FROM vast."{{SCHEMA}}".assets a
WHERE a.contamination_risk IS NOT NULL
ORDER BY
    CASE a.contamination_risk
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'low' THEN 3
        WHEN 'none' THEN 4
        ELSE 5
    END,
    a.processing_depth DESC
LIMIT {{LIMIT}}
