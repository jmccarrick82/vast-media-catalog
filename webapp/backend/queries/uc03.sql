-- UC03: Unauthorized Use Detection
-- Joins assets with hash_matches to surface unauthorized copies,
-- showing similarity scores and the matched asset details.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    hm.match_id,
    hm.asset_b_id AS matched_asset_id,
    b.s3_path AS matched_s3_path,
    b.filename AS matched_filename,
    hm.match_type,
    hm.similarity_score,
    hm.detected_at,
    a.unauthorized_match_count
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".hash_matches hm
    ON a.asset_id = hm.asset_a_id
LEFT JOIN vast."{{SCHEMA}}".assets b
    ON hm.asset_b_id = b.asset_id
WHERE hm.match_type IN ('unauthorized_copy', 'unauthorized_derivative', 'pirated_copy')
ORDER BY hm.similarity_score DESC, hm.detected_at DESC
LIMIT {{LIMIT}}
