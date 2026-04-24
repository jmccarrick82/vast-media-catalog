# VAST Data — Content Provenance Graph
## Use Cases by Buyer Persona

The asset relationship graph — built by VAST automatically at ingest from cryptographic, analytical, and filesystem signals — unlocks a different set of high-value capabilities depending on who you're talking to. This document maps each use case to the buyer persona most likely to care about it.

---

## Persona 1: Legal & Business Affairs

*Rights managers, licensing teams, business affairs executives, general counsel*

**Core problem:** Rights conflicts, unauthorized use, and unclear content lineage create legal and financial exposure. Most organizations can't answer "where did this content come from and what can we do with it?" without days of manual research.

---

### Use Case 1: Rights Conflict Detection

A clip is about to be published. Query the graph to trace it back to its camera original. The rights record on that original says: licensed for digital only, not broadcast. The derivative inherits that restriction automatically.

Without the graph, this conflict is invisible until a rights holder sends a cease-and-desist. With it, the conflict surfaces before publication.

**Value:** Prevents licensing violations before they happen. Eliminates the "we didn't know" defense.

---

### Use Case 2: Orphaned Asset Resolution

An archive has thousands of clips with no rights information attached — acquired through mergers, format migrations, or staff turnover. The graph traces them back to originals that *do* have rights records. Derivatives inherit the parent's rights status automatically.

**Value:** A large portion of "unknown rights" assets resolve without manual research, simply by tracing lineage to known assets.

---

### Use Case 3: Unauthorized Use Detection

A clip surfaces online that looks like your content. Hash it, run perceptual matching against the graph, find the source asset, check the rights record — no license was issued to that publisher.

You now have a chain of evidence: here is the original, here is the derivative, here is the match, here is the absence of a license.

**Value:** Actionable evidence for enforcement without manual investigation. The provenance chain is pre-built.

---

### Use Case 4: License Audit Trail

A licensor audits your usage of their content. Instead of manually searching through edit projects and delivery logs, query the graph: *show me every asset derived from any clip owned by [licensor], including every delivery, broadcast, and clip extract.*

**Value:** Audit response in minutes rather than weeks. Complete, defensible documentation of all usage.

---

### Use Case 5: Talent & Music Residuals

Union contracts (SAG-AFTRA, IATSE, AFM) require residual payments when content is reused. The graph records every time a clip containing a specific performance or music cue was re-edited or re-published.

Residuals can be calculated programmatically from the graph rather than relying on manual logging that inevitably misses uses.

**Value:** Accurate residual calculation. Reduced union disputes. Audit-ready documentation of every re-use event.

---

### Use Case 21: Chain of Custody for Legal Hold

Litigation requires preserving all relevant content in its original, unmodified state. The graph identifies: these 23 files are derivatives of the 3 camera originals relevant to this case.

Place a legal hold on all 26 with confidence that nothing relevant was missed. Hash records prove the files have not been modified since the hold was placed.

**Value:** Defensible legal holds. No risk of missing related content. Cryptographic proof of file integrity throughout the hold period.

---

### Use Case 26: Co-Production Attribution

International co-productions involve complex ownership splits across territories and production entities. The graph tracks which camera originals came from which production company's crew, which edits were made by which post-production house, and what the final delivery contains.

Ownership attribution becomes a query rather than a contractual dispute.

**Value:** Reduced co-production disputes. Clear, evidence-based attribution for revenue sharing and rights negotiations.

---

## Persona 2: Archive & Library

*Archive managers, library directors, media asset managers, digitization teams*

**Core problem:** Large archives contain enormous value that can't be accessed because relationships between assets were never recorded. Deletion decisions are risky, storage costs are uncontrolled, and re-use of existing content requires manual research.

---

### Use Case 6: Duplicate Storage Elimination

The graph immediately identifies every file that is a copy or near-copy of another file. In large archives, 30–50% of storage is often duplicates — the same master stored in multiple locations, the same proxy generated twice, the same clip exported by multiple editors who didn't know others had done it.

The graph allows safe consolidation with confidence that the highest-quality version is retained and all derivatives still resolve correctly.

**Value:** 20–40% storage cost reduction. No risk of accidentally losing the wrong copy.

---

### Use Case 7: Safe Deletion

Before deleting any file, query the graph: does anything depend on this? If it's a leaf node with no dependents — safe to delete. If it's a master with 47 derivatives — deleting it breaks the archive.

This is the single most common cause of catastrophic archive accidents. The graph makes the dependency chain visible before any action is taken.

**Value:** Eliminates accidental deletion of assets that other content depends on. Enables confident storage reclamation.

---

### Use Case 8: Master vs. Derivative Classification

Large archives often have no idea which version of a file is the authoritative master. The graph answers this definitively:

- Root node (no parents, many children) → camera original or master
- Leaf node (parents exist, no children) → delivery output, safe to expire if master is retained
- Middle node → intermediate derivative, keep if masters are present

Build a tiered storage policy on top of this: masters on fast NVMe, delivery outputs on cheaper object storage — automatically and confidently.

**Value:** Principled tiered storage. No guessing about what's safe to move to cold storage.

---

### Use Case 9: Archive Re-Conformation

A broadcaster wants to re-edit a program from 10 years ago at 4K. The delivered version exists but the original camera files were assumed lost.

The graph finds: these 47 clips in a forgotten directory on a cold storage volume have perceptual hash matches to sequences in the delivered program. The originals were never lost — they just weren't connected to anything.

**Value:** Recovery of content thought to be lost. Enables 4K re-masters and re-edits from archives that were previously considered unusable.

---

### Use Case 10: Version Control Across the Lifecycle

When a program goes through 12 edit versions before delivery, the graph tracks which version became the broadcast master, which became the streaming version, which became the international co-production cut.

When a correction is needed 2 years later, the query is: *what is the exact version that aired in Germany on March 9th 2024?* — returning a direct pointer, not a folder full of files named `v7_FINAL_REAL_v2_USE_THIS.mp4`.

**Value:** Authoritative version history without relying on naming conventions or human memory.

---

## Persona 3: AI & Data Science

*ML engineers, data scientists, AI pipeline architects, model governance teams*

**Core problem:** AI in media requires large, clean, well-understood training datasets. Without provenance, you can't know what's in your training data, whether you had the right to use it, or whether model outputs are contaminated by problematic inputs.

---

### Use Case 11: Training Data Provenance

A rights dispute arises: did you have the right to use that content for AI training? The graph traces every file that went into the training dataset back to its rights record. You can prove which content was licensed for AI training and which wasn't.

**Value:** Legal defensibility for AI training data usage. Increasingly critical as AI training litigation accelerates globally.

---

### Use Case 12: Model Contamination Detection

You want to train a model on camera-original, unprocessed footage only. The graph filters the training corpus to root nodes — files with no parents. No transcodes, no re-edits, no content that passed through a generative AI tool upstream.

**Value:** Clean, verifiable training data provenance. Prevents models from learning artifacts introduced by upstream processing.

---

### Use Case 13: Synthetic Content Tracking

Every time a generative AI tool produces an output that enters the archive, the graph records it as a node with `origin: AI_GENERATED` and links it to the prompt, model version, and tool used.

Over time you can query: how much of our archive is AI-generated? Which AI-generated assets have been published? Which have been used as training data for subsequent models?

The circular dependency problem — AI trained on AI outputs — becomes detectable and manageable.

**Value:** Governance visibility into synthetic content proliferation. Prevents AI model collapse from synthetic training data feedback loops.

---

### Use Case 14: Bias Audit

A news organization wants to audit whether AI-assisted editing tools introduced systematic bias in how certain subjects were portrayed. The graph connects the AI tool's outputs back to the specific model version, the training data it used, and every downstream publication.

Trace a potential bias from the published output back to the model version and the training corpus in one query chain.

**Value:** Auditable AI accountability. Evidence chain for regulatory compliance and editorial standards review.

---

## Persona 4: Production & Post-Production

*Producers, editors, post-production supervisors, localization teams, compliance reviewers*

**Core problem:** Finding the right footage takes too long. Clearance research is duplicated. Compliance decisions made on source clips don't follow the content into derivatives. Localization updates are a coordination nightmare.

---

### Use Case 15: Re-Use Discovery

A producer needs B-roll for a new production. Instead of sending a researcher to manually browse the archive, they query: *show me all clips semantically related to "urban street markets at night in Southeast Asia," ordered by quality, filtered to clips with confirmed clear rights.*

The graph combines CLIP embedding similarity with rights metadata to return a qualified shortlist in seconds.

**Value:** Research time reduced from days to seconds. Higher quality results because the query is semantic, not tag-dependent.

---

### Use Case 16: Clearance Inheritance

A clip is flagged in post because it may contain a copyrighted artwork visible in the background. Query the graph: which other productions have used clips from this same shoot?

If that artwork appears in 14 other programs that have already been cleared, the clearance research is already done — you inherit the result.

**Value:** Dramatically accelerates clearance for common assets. Eliminates redundant research across productions.

---

### Use Case 17: Compliance Propagation

A program has been through compliance review — watershed clearance, age rating, content warnings. The graph knows which clips made up that program.

When those same clips are re-used in a new production, the compliance data travels with them as inherited attributes. You start from "these clips were previously rated 15+ for violence" rather than from zero.

**Value:** Compliance review time reduced. Consistent ratings across productions using the same source material.

---

### Use Case 18: Localization Management

A program has been dubbed into 12 languages. The graph links each dubbed version back to the original and to each other.

When the original gets a correction — a factual error that needs to be re-recorded and re-edited — you instantly know all 12 localized versions need updating and exactly which segment in each needs to change.

**Value:** Eliminates missed localization updates. Reduces the cost and coordination overhead of corrections across multilingual content libraries.

---

## Persona 5: Security & IT

*CISOs, IT directors, infrastructure security teams, data governance officers, compliance officers*

**Core problem:** You can't protect what you can't see. Leak investigations are slow, GDPR blast radius is unknown, legal holds are incomplete, and ransomware impact assessment is guesswork.

---

### Use Case 19: Leak Investigation

A pre-release clip surfaces online. Hash it, run perceptual matching, find the source in the archive, query the graph: which delivery outputs were made from this source? Which systems did those outputs pass through?

Narrow the investigation from "everyone who had access to anything" to "specifically, these 3 delivery packages, sent to these 3 recipients, on this date."

**Value:** Leak investigation reduced from weeks to hours. Precise identification of the breach point rather than broad access reviews.

---

### Use Case 20: Regulatory Compliance (GDPR / AI Act)

GDPR and emerging AI content regulations require knowing where personal data — including biometric data like faces — appears in your content and what was done with it.

The graph maps every clip containing a detected individual to every derivative and delivery. If someone exercises their right to erasure, you know the full blast radius immediately: these 47 clips need review, across these 12 programs.

**Value:** Defensible GDPR compliance. Audit-ready documentation of where personal data exists and how it flows through the content lifecycle.

---

### Use Case 21: Chain of Custody for Legal Hold

*(Shared with Legal — see above)*

The graph identifies all files relevant to litigation and proves via cryptographic hash records that those files have not been modified since the hold was placed.

**Value:** IT can execute legal holds with confidence. No risk of missing related content. No risk of inadvertent modification.

---

### Use Case 22: Cybersecurity — Ransomware Impact Assessment

A ransomware attack encrypts a portion of the archive. The graph immediately tells you:

- Which encrypted files are masters with unaffected derivatives elsewhere → recoverable from derivatives
- Which encrypted files are unique originals with no copies → true losses requiring restoration from backup
- Which unencrypted files are derivatives of encrypted masters → at risk if not isolated

Triage is based on actual dependency relationships rather than guesswork about what's important.

**Value:** Faster, smarter ransomware response. Recovery effort focused on true originals with no surviving copies. Board-level incident reporting based on actual impact, not file counts.

---

## Persona 6: Business & Finance

*CFOs, COOs, heads of content strategy, M&A teams, insurance, business development*

**Core problem:** Content libraries are major balance sheet assets that are almost impossible to value accurately. Re-use revenue is undercounted. Insurance claims are imprecise. M&A due diligence is slow and incomplete.

---

### Use Case 23: Content Valuation

Which assets in the archive have been re-used most frequently? The graph counts derivatives, re-uses, deliveries, and syndication events per asset.

A clip used in 340 productions over 20 years has demonstrable commercial value with a documented history. A clip never referenced by anything is a candidate for cold storage or expiration. Assign commercial value scores to archive assets based on actual usage history, not assumption.

**Value:** Data-driven archive valuation. Better decisions about what to license, what to promote, what to retire.

---

### Use Case 24: Syndication Revenue Tracking

Content is licensed to 50 broadcasters in 30 countries. Each licensee received a specific version. The graph links each syndicated version back to the master, through the delivery package, to the license agreement.

When a licensee broadcasts the content, the audit trail connects that broadcast event to the specific asset version, confirming correct usage against license terms.

**Value:** Complete revenue audit trail. Faster identification of under-reporting or unauthorized usage by licensees.

---

### Use Case 25: Insurance & Disaster Recovery Valuation

A flood or fire destroys physical media. The insurer asks: what was lost, and what was it worth?

The graph tells you:
- Which destroyed files were unique masters with no surviving digital copies
- Which were derivatives whose masters survive
- The complete usage and commercial history of each lost asset

The difference between "we lost 50,000 files" and "we lost 847 unique masters representing these productions with this commercial history" is the difference between a rough estimate and a defensible insurance claim.

**Value:** Accurate insurance claims. Faster settlements. Evidence-based disaster recovery prioritization.

---

### Use Case 26: Co-Production Attribution

*(Shared with Legal — see above)*

Complex co-production ownership splits become a graph query rather than a contractual dispute. Track which content came from which production entity and what the revenue split should be based on actual content contribution.

**Value:** Faster co-production revenue reconciliation. Reduced disputes. Clear evidence for arbitration if disputes do arise.

---

## Summary Table

| Persona | Primary Question | Use Cases | Key VAST Capability |
|---------|-----------------|-----------|---------------------|
| Legal / Business Affairs | Where did this content come from and what can we do with it? | 1, 2, 3, 4, 5, 21, 26 | Rights inheritance, license audit, legal hold |
| Archive / Library | What do we have, where is it, and what depends on what? | 6, 7, 8, 9, 10 | Safe deletion, master classification, re-conformation |
| AI / Data Science | What is my training data made of and where did it come from? | 11, 12, 13, 14 | Training provenance, synthetic content tracking, bias audit |
| Production / Post | Can I find the right footage and does it come pre-cleared? | 15, 16, 17, 18 | Semantic re-use discovery, clearance inheritance, localization tracking |
| Security / IT | What is the blast radius of this incident? | 19, 20, 21, 22 | Leak tracing, GDPR mapping, ransomware triage |
| Business / Finance | What is our content library actually worth? | 23, 24, 25, 26 | Usage-based valuation, syndication audit, insurance documentation |

---

## The Unifying Insight

Every persona above is asking a version of the same question: **what do we know about our content, and how does it connect to everything else?**

The relationships between files are as valuable as the files themselves. Right now, almost no media organization has those relationships in a queryable form. The content exists. The history exists in fragments — across MAM databases, NLE project files, email threads, and people's memories.

VAST sits underneath all of it at the storage layer. It is the only place in the stack where you can build the complete picture without requiring every upstream system to change how it works. The graph is built automatically, stored in VASTDB, and queryable by any system, any persona, any workflow — using standard SQL.

---

*Document prepared for VAST Data NAB 2026 planning.*
