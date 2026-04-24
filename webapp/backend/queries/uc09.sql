-- UC09: Archive Re-Conformation
-- Shows assets with reconformation matches from hash_matches,
-- indicating content that can be re-conformed for new deliverables.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.reconformation_match_count,
    a.reconformation_viable,
    hm.match_id,
    hm.asset_b_id AS reconformation_candidate_id,
    b.s3_path AS candidate_s3_path,
    b.filename AS candidate_filename,
    hm.similarity_score,
    hm.reconformation_viable AS match_reconformation_viable,
    hm.detected_at
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".hash_matches hm
    ON a.asset_id = hm.asset_a_id
LEFT JOIN vast."{{SCHEMA}}".assets b
    ON hm.asset_b_id = b.asset_id
WHERE hm.reconformation_viable = true
ORDER BY hm.similarity_score DESC
LIMIT {{LIMIT}}
