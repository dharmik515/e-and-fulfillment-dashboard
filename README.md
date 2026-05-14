# E& Fulfillment Dashboard

Streamlit dashboard that auto-fills the E& / Etisalat PO #1147979-1 fulfillment
view from four uploaded workbooks:

1. **E& Project Guidelines** — PO + Packed Devices Details + Blocked Devices
   (drives the `RQ` / `CT` / `BL` counts).
2. **Master Template** — physical inventory: adds `Location / Bin / Stack`.
3. **Phone Check Bulk Lookup** — diagnostic source of truth: `Battery %`, `Grade`,
   `Regulatory Model Number` → `A-number`.
4. **Stack Bulk Upload** — adds `Deal Id` and `Latest Assessed Grade`.

Read-only — no manual entry. Every cell is computed from the source files.

## Local run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Tabs

- **Status by Model** — per-family rollup and per-SKU status across all 35 SKUs.
- **SKU Drill-down** — pick a SKU, see packed/blocked IMEIs with full enrichment
  and inferred block reasons.
- **Eligible Candidates** — devices in the uploaded data that match an in-scope
  SKU, pass all E& criteria (A-number, Grade ∈ {A+, A, B}, Battery ≥ 85%,
  MDM off, no Apple ID, 100% Working), and are not already in Packed/Blocked.
