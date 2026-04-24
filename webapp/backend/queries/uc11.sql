-- UC11: Training Data Provenance
-- Shows assets used as AI/ML training data, with dataset IDs,
-- rights clearance status, and processing chain details.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.training_dataset_id,
    a.is_training_original,
    a.rights_cleared_for_training,
    a.training_processing_chain,
    a.training_logged_at,
    a.license_type,
    a.asset_classification
FROM vast."{{SCHEMA}}".assets a
WHERE a.training_dataset_id IS NOT NULL
ORDER BY a.training_logged_at DESC
LIMIT {{LIMIT}}
