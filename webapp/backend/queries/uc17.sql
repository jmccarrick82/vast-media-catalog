-- UC17: Compliance Propagation
-- Shows assets with compliance ratings and content warnings,
-- tracking how compliance status propagates through related assets.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.compliance_rating,
    a.content_warnings,
    a.compliance_inherited_from,
    a.compliance_propagated_at,
    p.filename AS inherited_from_filename,
    p.compliance_rating AS parent_compliance_rating,
    p.content_warnings AS parent_content_warnings
FROM vast."{{SCHEMA}}".assets a
LEFT JOIN vast."{{SCHEMA}}".assets p
    ON a.compliance_inherited_from = p.asset_id
WHERE a.compliance_rating IS NOT NULL
ORDER BY a.compliance_propagated_at DESC
LIMIT {{LIMIT}}
