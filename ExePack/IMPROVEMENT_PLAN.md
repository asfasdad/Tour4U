## Improvement Plan

### Completed in this pass
- Standardized ID extraction from URL parameters for campaign and adgroup mapping.
- Rewrote `广告组ID` assignment to use URL IDs first, then domain as fallback.
- Rewrote runtime history directory path to be absolute and stable in both source mode and packaged mode.
- Updated Streamlit deprecated `use_container_width` usages to `width="stretch"`.

### Remaining optimization candidates
- Add deterministic unit tests for URL ID extraction and Shopify parser edge cases.
- Add retry/backoff strategy for network fetch in `consumer_worker`.
- Separate crawler, storage, and UI concerns into modules to reduce `main.py` size.
- Add structured logging for failed fetch/database writes for later audit.

### Missing feature candidates
- Campaign/adgroup timeline trend view with explicit ID drill-down.
- Duplicate-ad merge policy controls in UI (strict/loose dedup modes).
- Snapshot replay and manual re-parse workflow for `B_PENDING` records.
