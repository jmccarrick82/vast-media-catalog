-- UC13: Synthetic Content Tracking
-- Shows assets with non-zero AI probability, ordered by likelihood
-- of being AI-generated, with tool and model detection details.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.ai_probability,
    a.ai_tool_detected,
    a.ai_model_version,
    a.ai_detection_method,
    a.ai_detected_at,
    a.asset_classification
FROM vast."{{SCHEMA}}".assets a
WHERE a.ai_probability > 0
ORDER BY a.ai_probability DESC
LIMIT {{LIMIT}}
