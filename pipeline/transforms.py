"""
pipeline.transforms
===================
YOU WILL BUILD THIS INCREMENTALLY across Steps 4.1 → 4.4.

Roadmap:
    Step 4.1  → ParseWmsLine, ParseErpLine          (parsing + dead-letter)
    Step 4.2  → DeduplicateInvoicesDoFn             (stateful, with TTL timer)
    Step 4.3  → AssignEventTimestamp, VelocityFraudCheck (sliding window)
    Step 4.4  → load_vendor_baseline, AnomalyCheckDoFn   (side input + fallback)
    (Always) → Postgres sink DoFns (batched, idempotent)

Follow docs/phase1_walkthrough.html for ready-to-paste reference snippets.
"""
