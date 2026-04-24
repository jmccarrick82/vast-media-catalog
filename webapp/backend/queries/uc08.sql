-- UC08: Master vs Derivative Classification
-- Shows asset classification hierarchy by joining assets with relationships
-- to reveal master/derivative chains.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.asset_classification,
    a.classification_confidence,
    r.relationship_id,
    r.parent_asset_id,
    r.child_asset_id,
    r.relationship_type,
    r.confidence AS relationship_confidence,
    p.filename AS parent_filename,
    p.asset_classification AS parent_classification
FROM vast."{{SCHEMA}}".assets a
LEFT JOIN vast."{{SCHEMA}}".relationships r
    ON a.asset_id = r.child_asset_id
LEFT JOIN vast."{{SCHEMA}}".assets p
    ON r.parent_asset_id = p.asset_id
WHERE a.asset_classification IS NOT NULL
ORDER BY a.asset_classification, a.classification_confidence DESC
LIMIT {{LIMIT}}
