# AgriNexus — Intern Edition

A small, self-contained demo of the **AgriNexus Ecosystem Intelligence**
pipeline: matching, fair pricing, logistics, and trust/fraud — wired together
through a shared event stream, with a clean web UI for testing every module.

This edition is designed for interns: **Python standard library only**
(nothing to install), a **small dummy dataset**, and the full project report
in `PROJECT_REPORT.md`.

## Run it

```bash
python3 server.py
# → open http://localhost:8765
```

That single command starts the web UI. Use the four station tiles, the
"Run the full pipeline" button, or the **Run a test scenario** form to feed
your own lot/order into every model.

## What's inside

| File | Purpose |
|---|---|
| `server.py` | Web server + all four models (stdlib only) |
| `web/index.html` | The test-bench dashboard (open `http://localhost:8765`) |
| `dummy_data.json` | Crops, districts, grades, weights, vehicles, farmers |
| `dummy_mandi.json` | Sample mandi reference rates (₹/quintal) per district/crop |
| `dummy_towns.json` | ~70 Indian towns with coordinates (for distances) |
| `PROJECT_REPORT.pdf` | Concise project report for college submission (print-ready) |

## The four models

1. **Matching** — scores every farmer-lot ↔ buyer-order pair on 5 factors
   (price fit, freshness, distance, trust, quantity fit) with buyer-type
   weights, then picks the best pairs greedily.
2. **Pricing** — builds a fair price band around the mandi market rate
   (±6%, grade premium, logistics cost, 2% commission) and forecasts 5 days.
3. **Logistics** — plans the route farm → delivery hub, picks the smallest
   fitting vehicle (refrigerated first for perishables in heat), and scores
   spoilage risk.
4. **Trust & Fraud** — a rolling trust score, a tamper-evident hash-chain
   crop ledger, and 5 fraud rules (weight mismatch, price outlier, repeated
   cancellations, geofence inconsistency, circular trading).

Every step is published as an event on a shared stream — the right-hand
"Activity" panel in the UI.

## For interns: things to try extending

- Add a new fraud rule (e.g. "delivery far before scheduled time")
- Add a new buyer type with its own weights in `dummy_data.json`
- Replace greedy matching with a real assignment algorithm
- Persist the event log to a file (SQLite `json` dump)
