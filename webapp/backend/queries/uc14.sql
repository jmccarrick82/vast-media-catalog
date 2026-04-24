-- UC14: Bias Audit
-- Shows assets that have undergone bias auditing, with model details,
-- audit results, and risk levels for AI fairness compliance.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.bias_model_id,
    a.bias_ai_tool_used,
    a.bias_training_data_ids,
    a.bias_audit_result,
    a.bias_risk_level,
    a.bias_audited_at,
    a.ai_probability
FROM vast."{{SCHEMA}}".assets a
WHERE a.bias_audit_result IS NOT NULL
ORDER BY
    CASE a.bias_risk_level
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'low' THEN 3
        ELSE 4
    END,
    a.bias_audited_at DESC
LIMIT {{LIMIT}}
