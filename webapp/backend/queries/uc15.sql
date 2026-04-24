-- UC15: Re-Use Discovery
-- Summary view of assets with semantic embeddings, showing which assets
-- have been analyzed for content similarity and re-use potential.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.has_semantic_embeddings,
    a.embedding_model_name,
    a.embedding_frame_count,
    a.embeddings_extracted_at,
    a.asset_classification,
    a.duration_seconds,
    a.reuse_count
FROM vast."{{SCHEMA}}".assets a
WHERE a.has_semantic_embeddings = true
ORDER BY a.embedding_frame_count DESC, a.embeddings_extracted_at DESC
LIMIT {{LIMIT}}
