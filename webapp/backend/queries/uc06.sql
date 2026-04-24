-- UC06: Duplicate Storage Elimination
-- Joins assets with hash_matches to show exact and near-duplicate pairs,
-- along with potential storage savings in bytes.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.file_size_bytes,
    hm.match_id,
    hm.asset_b_id AS duplicate_asset_id,
    b.s3_path AS duplicate_s3_path,
    b.filename AS duplicate_filename,
    b.file_size_bytes AS duplicate_file_size_bytes,
    hm.match_type,
    hm.similarity_score,
    hm.storage_savings_bytes,
    a.duplicate_count,
    a.total_storage_savings_bytes
FROM vast."{{SCHEMA}}".assets a
JOIN vast."{{SCHEMA}}".hash_matches hm
    ON a.asset_id = hm.asset_a_id
LEFT JOIN vast."{{SCHEMA}}".assets b
    ON hm.asset_b_id = b.asset_id
WHERE hm.match_type IN ('exact_duplicate', 'near_duplicate', 'perceptual_duplicate')
ORDER BY hm.storage_savings_bytes DESC
LIMIT {{LIMIT}}
