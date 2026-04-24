# VAST Media Catalog: Content Provenance Use Cases

## The Problem

Media and entertainment companies manage millions of digital assets — master recordings, derivatives, localized versions, promotional clips, and AI-generated content — spread across disconnected storage silos, proprietary MAM systems, and cloud archives. Without unified provenance tracking, organizations face:

- **Rights violations** costing millions in litigation and settlement fees
- **Duplicate storage** wasting 20-40% of capacity budgets
- **AI governance gaps** exposing studios to regulatory penalties under the EU AI Act
- **Lost content lineage** making it impossible to trace how assets evolve across production workflows
- **Weeks-long forensic investigations** when leaks or security incidents occur

Traditional approaches require stitching together metadata from 5-10 separate systems — DAMs, MAMs, rights databases, storage platforms, and custom scripts — with no single source of truth.

## The VAST Data Platform Solution

VAST Data's Media Catalog leverages the **VAST DataEngine** to run automated analysis pipelines directly on content as it lands in storage. Every file upload triggers a chain of serverless functions that extract metadata, compute perceptual hashes, detect AI-generated content, analyze audio tracks, build relationship graphs, and split video into subclips — all without moving data off the platform.

The result is a **unified provenance database** queryable via standard SQL (Trino), powering 26 use cases across 6 personas — from Legal to AI/ML to Security — all from a single VAST namespace.

---

## Personas & Use Cases

### 1. Legal & Business Affairs

*Rights managers, licensing teams, business affairs executives, general counsel*

---

#### UC01 — Rights Conflict Detection

**The Challenge Today:**
Rights conflicts — overlapping territorial licenses, expired clearances applied to active content, or contradictory usage terms — are typically discovered only when a distributor or legal team manually audits a spreadsheet. By that point, content may have already been delivered to markets where the studio has no rights, triggering costly takedowns and legal exposure.

**VAST Value:**
VAST automatically tracks license type, territory restrictions, expiry dates, and conflict flags on every asset. When a conflict is detected (e.g., an asset licensed exclusively to Region A is found in a Region B delivery package), it surfaces immediately in the provenance database. Legal teams query a single table instead of cross-referencing three separate rights management systems.

---

#### UC02 — Orphaned Asset Resolution

**The Challenge Today:**
Orphaned assets — files with no clear parent, project, or owner — accumulate silently in storage. They cannot be safely deleted (they might be critical), cannot be relicensed (ownership is unclear), and cannot be included in audits (no metadata trail). Studios report 10-15% of their archive consists of orphaned content.

**VAST Value:**
VAST's relationship graph and hash-matching pipelines automatically link orphaned assets back to their parent content by matching perceptual hashes, file signatures, and temporal metadata. The resolution method and timestamp are recorded, turning unknown liabilities into cataloged, actionable assets.

---

#### UC03 — Unauthorized Use Detection

**The Challenge Today:**
Detecting unauthorized copies requires comparing content fingerprints across the entire library — a computationally expensive operation that most organizations run infrequently (quarterly or never). Pirated derivatives, unauthorized re-edits, and unlicensed copies circulate undetected for months.

**VAST Value:**
VAST's hash-comparator pipeline runs automatically on every new upload, comparing SHA-256 and perceptual hashes (pHash) against the full asset library. Matches are classified as `unauthorized_copy`, `unauthorized_derivative`, or `pirated_copy` with similarity scores, enabling real-time detection rather than periodic audits.

---

#### UC04 — License Audit Trail

**The Challenge Today:**
During M&A transactions, content licensing audits, or distribution renewals, legal teams must reconstruct the complete licensing history of thousands of assets. This typically requires weeks of manual work pulling records from multiple systems, with no guarantee of completeness.

**VAST Value:**
Every asset carries its full license audit trail — licensor, license type, usage type, derivative counts, territories, and expiry — all timestamped and queryable via SQL. A complete audit that previously took weeks can now be generated in seconds with a single query.

---

#### UC05 — Talent & Music Residuals

**The Challenge Today:**
Residual payments to talent (actors, musicians, voice artists) depend on accurately tracking which people and music appear in which assets, at what timecodes, and across how many derivative works. Manual logging is error-prone, and missed detections lead to underpayment disputes or overpayment waste.

**VAST Value:**
VAST's audio-analyzer and face-detector pipelines automatically catalog every talent appearance and music segment with frame-level precision, confidence scores, and duration. The `talent_music` table provides a complete, machine-verified record for residual calculations — no manual timecode logging required.

---

#### UC21 — Chain of Custody for Legal Hold

**The Challenge Today:**
When content is placed under legal hold (litigation, regulatory investigation, IP dispute), organizations must prove the content has not been tampered with. Traditional approaches rely on periodic manual hash checks, with no continuous integrity monitoring. If a hash changes between checks, there is no audit trail of when or how the modification occurred.

**VAST Value:**
VAST records the SHA-256 hash at the moment legal hold is placed (`sha256_at_hold`) and continuously compares it against the current hash. Any modification is detected automatically, and the integrity status (INTACT, MODIFIED, NO_BASELINE) is always current. This provides an unbroken, tamper-evident chain of custody that stands up in legal proceedings.

---

#### UC26 — Co-Production Attribution

**The Challenge Today:**
Co-productions involve multiple production companies, each with specific ownership splits, contribution types (financing, creative, distribution), and territorial rights. Tracking attribution across hundreds of assets with dozens of partners typically requires custom spreadsheets that quickly become stale and inconsistent.

**VAST Value:**
VAST's `production_entities` table maintains per-asset, per-company ownership splits with contribution types and crew origin data. Attribution is computed automatically as assets flow through production, ensuring every partner's contribution is accurately recorded from day one — not reconstructed months later.

---

### 2. Archive & Library

*Archive managers, library directors, media asset managers, digitization teams*

---

#### UC06 — Duplicate Storage Elimination

**The Challenge Today:**
Media libraries accumulate exact and near-duplicate files across projects, deliveries, and archive migrations. Without automated deduplication, studios routinely store 3-5 copies of the same content, wasting millions in storage costs annually. Manual identification is impractical at scale — a typical studio has 50-100M files.

**VAST Value:**
VAST's hash-comparator pipeline identifies exact duplicates (SHA-256 match), near-duplicates (high pHash similarity), and perceptual duplicates (visually identical but different encodings) automatically. Each match includes a storage savings estimate in bytes, enabling archive teams to reclaim 20-40% of capacity with confidence that no unique content is lost.

---

#### UC07 — Safe Deletion

**The Challenge Today:**
Deleting assets from a media library is risky — an asset that appears unused may be a master referenced by dozens of derivatives, a component in an active production, or the only surviving copy of archived content. Without dependency analysis, archive teams either never delete (costs spiral) or delete unsafely (content is lost).

**VAST Value:**
VAST's graph-analyzer pipeline computes the full dependency tree for every asset — dependent count, leaf/root status, and an explicit `deletion_safe` flag. Archive managers can confidently bulk-delete leaf nodes with zero dependents while protecting critical masters and in-use assets.

---

#### UC08 — Master vs. Derivative Classification

**The Challenge Today:**
Distinguishing masters from derivatives, proxies, and transcodes is fundamental to archive management, but classification is typically done manually or not at all. Without it, organizations cannot answer basic questions: "How many unique masters do we have?" or "Which derivatives belong to which master?"

**VAST Value:**
VAST automatically classifies every asset (original, derivative, subclip, transcode) with confidence scores and builds parent-child relationship graphs. The `relationships` table maps every derivative back to its master, enabling force-directed visualizations of the entire content hierarchy.

---

#### UC09 — Archive Re-Conformation

**The Challenge Today:**
Re-conformation — rebuilding a new deliverable from archived source material — requires finding archived assets that closely match the target content. This is typically a manual, time-consuming search through poorly indexed archives, often resulting in re-shoots or re-purchases of content that already exists in the library.

**VAST Value:**
VAST's hash-matching pipeline automatically identifies archive content viable for re-conformation by comparing perceptual similarity scores. Assets flagged as `reconformation_viable` can be immediately retrieved and re-purposed, saving production costs and accelerating delivery timelines.

---

#### UC10 — Version Control Across the Lifecycle

**The Challenge Today:**
Media assets go through dozens of versions — rough cuts, color grades, audio mixes, localized variants, delivery masters. Traditional file systems and MAMs lose version lineage, making it impossible to trace how an asset evolved, revert to a previous version, or understand which version was delivered to which distributor.

**VAST Value:**
VAST's `version_history` table maintains a complete version chain for every asset — version numbers, labels, timestamps, and links to previous versions. This provides a Git-like version history for media content, queryable via SQL, with full traceability from first ingest to final delivery.

---

### 3. AI & Data Science

*ML engineers, data scientists, AI pipeline architects, model governance teams*

---

#### UC11 — Training Data Provenance

**The Challenge Today:**
AI/ML models trained on media content face increasing scrutiny from regulators (EU AI Act), rights holders, and internal governance teams. Organizations cannot easily answer: "Which assets were used to train this model? Were they rights-cleared? What processing was applied before training?" Without this provenance, models face legal and ethical risk.

**VAST Value:**
VAST tracks training dataset IDs, rights clearance status, processing chains, and source classification for every asset used in model training. This provides a complete, auditable provenance trail from raw content to training dataset to deployed model — essential for regulatory compliance and responsible AI.

---

#### UC12 — Model Contamination Detection

**The Challenge Today:**
When AI-generated or AI-modified content enters a training pipeline, it can "contaminate" downstream models — a growing concern known as model collapse. Detecting contamination requires tracing processing depth and upstream AI involvement, which is impossible without automated lineage tracking.

**VAST Value:**
VAST's synthetic-detector and graph-analyzer pipelines automatically assess contamination risk for every asset, tracking AI probability, processing depth, and upstream AI processing flags. Assets are classified by risk level (high/medium/low/none), enabling ML teams to quarantine contaminated content before it enters training pipelines.

---

#### UC13 — Synthetic Content Tracking

**The Challenge Today:**
With AI-generated content (deepfakes, generative video, synthetic voices) becoming indistinguishable from authentic content, organizations need to identify synthetic assets in their library. Manual review does not scale, and metadata labels are easily stripped or forged.

**VAST Value:**
VAST's synthetic-detector pipeline analyzes every asset for AI generation probability, detecting the specific tool (Stable Diffusion, DALL-E, Midjourney, etc.) and model version used. Content is automatically segmented into AI-Generated (>70%), Uncertain (30-70%), and Likely Organic (<30%) categories, providing continuous, automated synthetic content monitoring.

---

#### UC14 — Bias Audit

**The Challenge Today:**
AI fairness audits require tracing which models processed which content, what training data was used, and whether outputs exhibit demographic or representational bias. This audit trail is typically fragmented across ML experiment trackers, model registries, and content databases with no unified view.

**VAST Value:**
VAST consolidates bias audit data — model IDs, AI tools used, training data references, audit results, and risk levels — directly on each asset record. This enables organization-wide bias audits via a single SQL query, replacing manual cross-referencing of disparate ML governance tools.

---

### 4. Production & Post-Production

*Producers, editors, post-production supervisors, localization teams*

---

#### UC15 — Re-Use Discovery

**The Challenge Today:**
Finding re-usable content in a large media library requires semantic understanding — not just filename or metadata search, but visual similarity. "Find all beach sunset footage" or "find clips similar to this hero shot" requires expensive, specialized search infrastructure that most organizations lack.

**VAST Value:**
VAST's clip-embedder pipeline extracts CLIP semantic embedding vectors from video frames, enabling content-based visual similarity search. Assets with embeddings are flagged with model name, frame count, and extraction timestamps, making the entire library searchable by visual content rather than just metadata keywords.

---

#### UC16 — Clearance Inheritance

**The Challenge Today:**
When a master asset is cleared for use, that clearance should propagate to its derivatives — but in practice, clearance status is tracked per-asset with no inheritance logic. Editors waste hours re-clearing derivatives that should automatically inherit their parent's clearance status.

**VAST Value:**
VAST's provenance graph automatically propagates clearance status through parent-child relationships. When a master is cleared, all derivatives inherit that clearance with a recorded chain (`clearance_inherited_from`), eliminating redundant clearance workflows and ensuring no derivative ships without proper authorization.

---

#### UC17 — Compliance Propagation

**The Challenge Today:**
Content compliance ratings (G, PG, PG-13, R, NC-17) and content warnings must be applied consistently across all versions of an asset — masters, derivatives, localized versions, promotional clips. In practice, compliance metadata is often inconsistent across versions, creating regulatory risk in age-gated markets.

**VAST Value:**
VAST propagates compliance ratings and content warnings through the relationship graph, ensuring every derivative inherits its parent's compliance classification. Inconsistencies are surfaced automatically, and the propagation chain is fully auditable — critical for platforms operating in multiple regulatory jurisdictions.

---

#### UC18 — Localization Management

**The Challenge Today:**
Managing localized content — dubbed versions, subtitle tracks, language-specific edits — requires tracking which languages exist for each asset, which source was dubbed from which original, and what subtitle tracks are embedded. This information is typically scattered across localization vendor spreadsheets and MAM custom fields.

**VAST Value:**
VAST's audio-analyzer pipeline automatically detects spoken language, catalogs embedded subtitle tracks, and links dubbed versions to their source assets. The `detected_language`, `language_confidence`, `subtitle_tracks`, and `dubbed_from_asset_id` fields provide a complete localization inventory queryable via SQL — no vendor spreadsheets required.

---

### 5. Security & IT

*CISOs, IT directors, infrastructure security teams, compliance officers*

---

#### UC19 — Leak Investigation

**The Challenge Today:**
When content leaks, forensic investigators must trace the leaked file back through the delivery chain to identify the source. This requires matching file fingerprints against delivery records — a process that can take weeks when delivery logs are spread across email, FTP logs, and distribution platform databases.

**VAST Value:**
VAST maintains a complete delivery chain for every asset — recipients, delivery dates, hash fingerprints, and distribution paths. When a leak occurs, investigators query a single table to match the leaked file's hash against all delivery records, reducing investigation time from weeks to minutes.

---

#### UC20 — Regulatory Compliance (GDPR / AI Act)

**The Challenge Today:**
GDPR and the EU AI Act require organizations to identify and track personal data in media content — faces, voices, biometric data. For a studio with millions of video files, manually cataloging which files contain which persons' data is impossible. Without this inventory, organizations cannot respond to data subject access requests or deletion requests within the required timeframes.

**VAST Value:**
VAST's face-detector pipeline automatically catalogs every person detected in every asset — face count, person IDs, data types, frame timestamps, and a "blast radius" metric showing how many assets contain a given person's data. This provides the complete PII inventory required for GDPR compliance, enabling data subject requests to be fulfilled in hours rather than months.

---

#### UC22 — Cybersecurity: Ransomware Impact Assessment

**The Challenge Today:**
After a ransomware attack, organizations must rapidly assess the impact: How many assets are affected? Which are critical and irreplaceable? Which have backups? Which are leaf nodes that can be regenerated from masters? Without this analysis, recovery prioritization is guesswork, and the most valuable content may be restored last.

**VAST Value:**
VAST's provenance database enables instant ransomware impact assessment. Assets are automatically classified by recovery priority (CRITICAL/HIGH/MEDIUM/LOW) based on uniqueness, backup status, derivative counts, and commercial value. Security teams get an immediate, aggregate view of the blast radius — how many assets per priority tier, total data volume, backup coverage gaps — enabling rapid, informed recovery decisions.

---

### 6. Business & Finance

*CFOs, COOs, heads of content strategy, M&A teams, insurance underwriters*

---

#### UC23 — Content Valuation

**The Challenge Today:**
Valuing a media library — for M&A, financing, or strategic planning — requires understanding which assets are premium, which are commodity, and how value distributes across the catalog. Traditional valuations rely on manual appraisals of sample assets, extrapolated to the full library, with no objective, data-driven methodology.

**VAST Value:**
VAST computes a commercial value score for every asset based on derivative count, reuse frequency, delivery count, and classification. Assets are tiered (PREMIUM/HIGH/MEDIUM/LOW), enabling data-driven library valuation at any point in time. An M&A due diligence that previously required 6-8 weeks of manual appraisal can now be generated instantly from the provenance database.

---

#### UC24 — Syndication Revenue Tracking

**The Challenge Today:**
Tracking which content has been syndicated to which licensees in which territories — and matching those records against revenue — requires reconciling data from distribution platforms, CRM systems, and financial databases. Discrepancies between what was delivered and what was paid for are common and costly to resolve.

**VAST Value:**
VAST's `syndication_records` table maintains per-asset, per-territory, per-licensee distribution records with delivery version IDs and license status. This provides a single source of truth for syndication activity, enabling automated revenue reconciliation and gap analysis across the entire distribution network.

---

#### UC25 — Insurance & Disaster Recovery Valuation

**The Challenge Today:**
Insuring a media library requires determining which assets are irreplaceable (original negatives, master recordings, one-of-a-kind archival footage) vs. replaceable (transcodes, proxies, derivatives that can be regenerated). Insurance underwriters typically receive rough estimates rather than precise, asset-level valuations.

**VAST Value:**
VAST automatically classifies every asset's irreplaceability, digital copy count, replacement cost tier, and commercial history score. Insurance teams get a precise, per-asset valuation with clear categorization of which content is truly irreplaceable vs. regenerable — enabling accurate premium calculations and informed disaster recovery prioritization.

---

## Architecture Summary

### DataEngine Pipeline

Every file uploaded to the VAST namespace triggers a chain of serverless functions:

| Stage | Function | What It Does |
|-------|----------|-------------|
| 1 | **metadata-extractor** | Extracts codec, resolution, duration, frame rate, file size |
| 2 | **hash-generator** | Computes SHA-256 cryptographic hash and pHash perceptual hash |
| 3 | **keyframe-extractor** | Extracts representative keyframes at scene boundaries |
| 4 | **video-subclip** | Splits long-form video into 30-second subclips for granular analysis |
| 5 | **hash-comparator** | Compares hashes against the full library for duplicates and matches |
| 6 | **synthetic-detector** | Analyzes AI generation probability, tool detection, metadata forensics |
| 7 | **graph-analyzer** | Builds relationship graphs, computes dependencies, classifies assets |
| 8 | **audio-analyzer** | Detects language, catalogs subtitle tracks, fingerprints audio |
| *Future* | **face-detector** | Detects and identifies faces for talent tracking and GDPR compliance |
| *Future* | **clip-embedder** | Extracts CLIP semantic embeddings for visual similarity search |

### Unified Data Model

All analysis results flow into a single provenance database — 8 tables, queryable via standard SQL through Trino:

| Table | Purpose |
|-------|---------|
| `assets` | Central asset record with 60+ metadata columns |
| `relationships` | Parent-child and derivative relationship graph |
| `hash_matches` | Duplicate, unauthorized copy, and similarity matches |
| `version_history` | Version chains across the asset lifecycle |
| `talent_music` | Talent appearances and music detections with timecodes |
| `gdpr_personal_data` | Per-person PII detections for regulatory compliance |
| `syndication_records` | Per-territory, per-licensee distribution records |
| `production_entities` | Co-production attribution and ownership splits |

### Why VAST

| Capability | Impact |
|-----------|--------|
| **Single Namespace** | One platform replaces 5-10 disconnected systems |
| **DataEngine Serverless Functions** | Analysis runs automatically on ingest — no external compute clusters |
| **SQL via Trino** | Standard SQL access for any BI tool, dashboard, or custom application |
| **S3 + NFS + HDFS** | Access content via any protocol without data movement |
| **Exabyte Scale** | Handles libraries of any size with linear performance scaling |
| **Zero-Copy Clones** | Instant snapshots for ML experiments without doubling storage |
| **Schema Evolution** | Add new analysis columns without downtime or migration |
| **QoS Policies** | Prioritize real-time ingest over batch analytics automatically |

---

*Built on VAST Data Platform with VAST DataEngine serverless functions, Trino SQL engine, and unified content provenance tracking.*
