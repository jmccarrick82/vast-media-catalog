-- UC19: Leak Investigation
-- Shows assets with delivery chain and recipient details for
-- tracing content leaks back through the distribution chain.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.delivery_recipient,
    a.delivery_date,
    a.delivery_chain,
    a.leak_hash_fingerprint,
    a.leak_indexed_at,
    a.sha256
FROM vast."{{SCHEMA}}".assets a
WHERE a.delivery_chain IS NOT NULL
ORDER BY a.leak_indexed_at DESC
LIMIT {{LIMIT}}
