-- UC18: Localization Management
-- Shows assets with detected languages and dubbing/subtitle information,
-- linking dubbed versions back to their source assets.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.detected_language,
    a.language_confidence,
    a.dubbed_from_asset_id,
    a.subtitle_tracks,
    a.localization_detected_at,
    src.filename AS dubbed_from_filename,
    src.s3_path AS dubbed_from_path,
    src.detected_language AS source_language
FROM vast."{{SCHEMA}}".assets a
LEFT JOIN vast."{{SCHEMA}}".assets src
    ON a.dubbed_from_asset_id = src.asset_id
WHERE a.detected_language IS NOT NULL
ORDER BY a.detected_language, a.localization_detected_at DESC
LIMIT {{LIMIT}}
