-- UC22: Cybersecurity -- Ransomware Impact Assessment
-- Groups assets by recovery priority level, showing counts, total size,
-- backup status, and unique original counts per priority tier.
SELECT
    a.recovery_priority,
    COUNT(*) AS asset_count,
    SUM(a.file_size_bytes) AS total_size_bytes,
    SUM(CASE WHEN a.is_unique_original = true THEN 1 ELSE 0 END) AS unique_originals,
    SUM(CASE WHEN a.has_backup = true THEN 1 ELSE 0 END) AS with_backup,
    SUM(CASE WHEN a.has_backup = false OR a.has_backup IS NULL THEN 1 ELSE 0 END) AS without_backup,
    AVG(a.surviving_derivatives_count) AS avg_surviving_derivatives
FROM vast."{{SCHEMA}}".assets a
WHERE a.recovery_priority IS NOT NULL
GROUP BY a.recovery_priority
ORDER BY
    CASE a.recovery_priority
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
        ELSE 5
    END
LIMIT {{LIMIT}}
