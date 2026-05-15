"""
E& Fulfillment Dashboard
========================

Auto-fills the "E& Order Overview" matrix (Model x Storage x Colour -> RQ/CT/BL)
from four uploaded workbooks:

  1. E& Project Guidelines  - PO + Packed Devices Details + Blocked Devices
                              (CT and BL counts come from THIS file only)
  2. Master Template        - enrichment: Location / Bin / Stack
  3. Phone Check Lookup     - enrichment: Battery %, Grade (source of truth),
                              Regulatory Model Number (A-number fallback)
  4. Stack Bulk Upload      - enrichment: A-number lookup, Existing stack/Dealer

Read-only dashboard. No manual data entry. The Overview matrix shape
(35 SKUs, 4 families x storage rows x 8 colour columns) is left untouched.
"""

import io
import re
import warnings
from collections import Counter
from typing import Optional

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# openpyxl tolerance shim
# The Stack Bulk file ships with an invalid `errorStyle` on a data-validation
# rule. Patch the descriptor so reading does not blow up.
# ---------------------------------------------------------------------------
try:
    from openpyxl.descriptors.base import NoneSet

    _orig_set = NoneSet.__set__

    def _safe_set(self, instance, value):
        try:
            _orig_set(self, instance, value)
        except Exception:
            instance.__dict__[self.name] = None

    NoneSet.__set__ = _safe_set
except Exception:
    pass

warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------------
# Canonicalisation (light fuzzy matching: case + whitespace tolerant)
# ---------------------------------------------------------------------------

_FAMILY_PATTERNS = [
    ("iPhone 15 Pro Max", re.compile(r"i\s*phone\s*15\s*pro\s*max", re.I)),
    ("iPhone 15 Pro",     re.compile(r"i\s*phone\s*15\s*pro(?!\s*max)", re.I)),
    ("iPhone 15 Plus",    re.compile(r"i\s*phone\s*15\s*plus", re.I)),
    ("iPhone 15",         re.compile(r"i\s*phone\s*15(?!\s*(?:pro|plus))", re.I)),
]

# A-number -> family canonical map (Apple regulatory model numbers for iPhone 15)
A_NUMBER_FAMILY = {
    "A3090": "iPhone 15",
    "A3094": "iPhone 15 Plus",
    "A3102": "iPhone 15 Pro",
    "A3106": "iPhone 15 Pro Max",
}

# Storage size canonical set
VALID_STORAGE = {"128GB", "256GB", "512GB", "1TB"}

# Colour canonical set (matches E& Overview matrix headers exactly)
COLOURS = [
    "Black", "Blue", "Green", "Yellow",
    "White Titanium", "Natural Titanium", "Black Titanium", "Blue Titanium",
]

_COLOUR_LOOKUP = {c.lower(): c for c in COLOURS}
# Common variants
_COLOUR_LOOKUP.update({
    "white": "White Titanium",        # only iPhone 15 Pro uses these; pure 'white' rare
    "natural": "Natural Titanium",
    "graphite": "Black Titanium",     # only used if seen on phone-check
})

def canon_family(text) -> Optional[str]:
    if not isinstance(text, str):
        return None
    for canon, pat in _FAMILY_PATTERNS:
        if pat.search(text):
            return canon
    return None


def canon_storage(text) -> Optional[str]:
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d+)\s*(GB|TB)", text, re.I)
    if not m:
        return None
    val = f"{m.group(1)}{m.group(2).upper()}"
    return val if val in VALID_STORAGE else None


def canon_colour(text) -> Optional[str]:
    if not isinstance(text, str):
        return None
    s = re.sub(r"\s+", " ", text.strip().lower())
    if s in _COLOUR_LOOKUP:
        return _COLOUR_LOOKUP[s]
    # token check: if any colour name is a substring, use it (longest first)
    for canon in sorted(COLOURS, key=len, reverse=True):
        if canon.lower() in s:
            return canon
    return None


def make_sku_key(family, storage, colour):
    return (canon_family(family), canon_storage(storage), canon_colour(colour))


# ---------------------------------------------------------------------------
# PO description parser
# "HANDSET IPHONE 15 PRO BLACK TITANIUM 256GB (RENEWED)"
#   -> (iPhone 15 Pro, 256GB, Black Titanium)
# ---------------------------------------------------------------------------

def parse_po_description(desc: str):
    if not isinstance(desc, str):
        return (None, None, None)
    s = re.sub(r"^\s*HANDSET\s+", "", desc, flags=re.I)
    s = re.sub(r"\s*\(RENEWED\)\s*$", "", s, flags=re.I)

    family = canon_family(s)
    storage = canon_storage(s)

    rest = s
    if family:
        for canon, pat in _FAMILY_PATTERNS:
            if canon == family:
                rest = pat.sub("", rest, count=1)
                break
    if storage:
        rest = re.sub(re.escape(storage), "", rest, flags=re.I)

    colour = canon_colour(rest)
    return (family, storage, colour)


# ---------------------------------------------------------------------------
# Loaders (cached on file bytes so the same upload isn't reparsed)
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def _read_all_sheets(file_bytes: bytes):
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, header=None)


@st.cache_data(show_spinner=False)
def _read_sheet(file_bytes: bytes, sheet_name, header=0):
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=header)


# ---------------------------------------------------------------------------
# E& workbook parsing
# ---------------------------------------------------------------------------

def parse_po_sheet(po_df: pd.DataFrame) -> pd.DataFrame:
    """Parse the 'PO' sheet. The header is on row index 1, data starts row 2,
    and the TOTAL row at the bottom should be dropped."""
    header = po_df.iloc[1].tolist()
    df = po_df.iloc[2:].copy()
    df.columns = header
    df = df[df["Line #"].astype(str).str.match(r"^\d+$", na=False)].reset_index(drop=True)
    df["Line #"] = df["Line #"].astype(int)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0).astype(int)

    parsed = df["Description"].apply(parse_po_description)
    df["Family"] = parsed.apply(lambda t: t[0])
    df["Storage"] = parsed.apply(lambda t: t[1])
    df["Colour"] = parsed.apply(lambda t: t[2])
    df["SKU Key"] = list(zip(df["Family"], df["Storage"], df["Colour"]))
    return df


def parse_packed_sheet(packed_df: pd.DataFrame) -> pd.DataFrame:
    """Packed Devices Details: columns IMEI, Deal Id, A Number, Model, Storage, Colour
    Header on row 0."""
    header = packed_df.iloc[0].tolist()
    df = packed_df.iloc[1:].copy()
    df.columns = header
    df = df.dropna(subset=["IMEI"]).reset_index(drop=True)
    df["IMEI"] = df["IMEI"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["Family"] = df["Model"].apply(canon_family)
    df["StorageC"] = df["Storage"].apply(canon_storage)
    df["ColourC"] = df["Colour"].apply(canon_colour)
    df["SKU Key"] = list(zip(df["Family"], df["StorageC"], df["ColourC"]))
    return df


def parse_blocked_sheet(blocked_df: pd.DataFrame) -> pd.DataFrame:
    """Blocked Devices: columns Deal Id, IMEI, Model, Storage, Colour, A number, Status
    Header on row 0. The sheet also has a side-legend in cols 8+ which we ignore."""
    header = blocked_df.iloc[0].tolist()
    df = blocked_df.iloc[1:].copy()
    df.columns = header
    # Keep only the first 7 logical columns
    keep = ["Deal Id", "IMEI", "Model", "Storage", "Colour", "A number", "Status"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].dropna(subset=["IMEI"]).reset_index(drop=True)
    df["IMEI"] = df["IMEI"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["Family"] = df["Model"].apply(canon_family)
    df["StorageC"] = df["Storage"].apply(canon_storage)
    df["ColourC"] = df["Colour"].apply(canon_colour)
    df["SKU Key"] = list(zip(df["Family"], df["StorageC"], df["ColourC"]))
    return df


def parse_checklist_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Packing & Dispatch Checklist - header on row 1."""
    header = df.iloc[1].tolist()
    out = df.iloc[2:].copy()
    out.columns = header
    out = out.dropna(subset=["Category"]).reset_index(drop=True)
    return out


def parse_material_sheet(df: pd.DataFrame) -> pd.DataFrame:
    header = df.iloc[1].tolist()
    out = df.iloc[2:].copy()
    out.columns = header
    out = out.dropna(subset=["Material"]).reset_index(drop=True)
    return out


def parse_po_overview(df: pd.DataFrame) -> dict:
    """Read the 'PO Overview' sheet into a flat dict of header -> value."""
    out = {}
    for _, row in df.iterrows():
        k, v = row.iloc[0], row.iloc[1]
        if isinstance(k, str) and pd.notna(v):
            out[k.strip()] = v
    return out


# ---------------------------------------------------------------------------
# Enrichment from dynamic files (Master / Phone Check / Stack Bulk)
# ---------------------------------------------------------------------------

def parse_master(master_df: pd.DataFrame) -> pd.DataFrame:
    df = master_df.copy()
    df["IMEI"] = df["IMEI"].astype(str).str.replace(r"\.0$", "", regex=True)
    return df


def parse_phone_check(pc_df: pd.DataFrame) -> pd.DataFrame:
    df = pc_df.copy()
    df["IMEI"] = df["IMEI"].astype(str).str.replace(r"\.0$", "", regex=True)
    # Phone Check can carry multiple test sessions per IMEI. Keep the most
    # recent (Updated Date) so each IMEI shows once in candidate lists.
    if "Updated Date" in df.columns:
        df["_ts"] = pd.to_datetime(df["Updated Date"], errors="coerce")
        df = (df.sort_values("_ts", na_position="first")
                .drop_duplicates(subset=["IMEI"], keep="last")
                .drop(columns="_ts")
                .reset_index(drop=True))
    else:
        df = df.drop_duplicates(subset=["IMEI"], keep="last").reset_index(drop=True)
    return df


def parse_stack_bulk(stack_df: pd.DataFrame) -> pd.DataFrame:
    df = stack_df.copy()
    df["IMEI Number"] = df["IMEI Number"].astype(str).str.replace(r"\.0$", "", regex=True)
    return df


def enrich_device_row(imei: str, master, phone_check, stack):
    """Look up an IMEI across the three dynamic files and return a flat dict
    of enrichment columns. Empty dict if no match."""
    out = {
        "Location": None, "Bin": None, "Room": None, "Stack": None,
        "Battery %": None, "PC Grade": None, "100% Working": None,
        "A-number": None, "MDM Status": None, "AppleID": None,
        "Existing Stack & Dealer": None, "Assessed Grade": None,
    }

    if master is not None and imei in master["IMEI"].values:
        mrow = master.loc[master["IMEI"] == imei].iloc[0]
        out["Location"] = mrow.get("Location")
        out["Bin"] = mrow.get("Bin")
        out["Room"] = mrow.get("Room")
        out["Stack"] = mrow.get("Stack")

    if phone_check is not None and imei in phone_check["IMEI"].values:
        prow = phone_check.loc[phone_check["IMEI"] == imei].iloc[0]
        out["Battery %"] = prow.get("Battery Health Percentage")
        out["PC Grade"] = prow.get("Grade")
        out["100% Working"] = prow.get("100% Working")
        out["MDM Status"] = prow.get("MDM Status")
        out["AppleID"] = prow.get("AppleID")
        reg = prow.get("Regulatory Model Number")
        if isinstance(reg, str):
            m = re.search(r"(A\d{4})", reg)
            if m:
                out["A-number"] = m.group(1)

    if stack is not None and imei in stack["IMEI Number"].values:
        srow = stack.loc[stack["IMEI Number"] == imei].iloc[0]
        out["Existing Stack & Dealer"] = srow.get("Existing stack Id & Dealer")
        out["Assessed Grade"] = srow.get("Latest Assessed Grade")

    return out


# ---------------------------------------------------------------------------
# Matrix build
# ---------------------------------------------------------------------------

# Display row order in the matrix (family + storage)
ROW_ORDER = [
    ("iPhone 15",         "256GB"),
    ("iPhone 15",         "512GB"),
    ("iPhone 15 Plus",    "128GB"),
    ("iPhone 15 Plus",    "256GB"),
    ("iPhone 15 Plus",    "512GB"),
    ("iPhone 15 Pro",     "128GB"),
    ("iPhone 15 Pro",     "256GB"),
    ("iPhone 15 Pro",     "512GB"),
    ("iPhone 15 Pro Max", "256GB"),
]


def build_overview_matrix(po_df, packed_df, blocked_df):
    """Return a DataFrame matching the E& Order Overview layout, with RQ/CT/BL
    columns under each colour."""
    rq_lookup = {sku: int(qty) for sku, qty in zip(po_df["SKU Key"], po_df["Qty"])}
    ct_lookup = Counter(packed_df["SKU Key"].tolist())
    bl_lookup = Counter(blocked_df["SKU Key"].tolist())

    columns = pd.MultiIndex.from_product(
        [COLOURS, ["RQ", "CT", "BL"]], names=["Colour", "Metric"]
    )
    rows = pd.MultiIndex.from_tuples(ROW_ORDER, names=["Model", "Storage"])

    data = []
    for (family, storage) in ROW_ORDER:
        row = []
        for colour in COLOURS:
            sku = (family, storage, colour)
            rq = rq_lookup.get(sku, None)
            ct = ct_lookup.get(sku, 0)
            bl = bl_lookup.get(sku, 0)
            row.append(rq if rq else "-")
            row.append(ct)
            row.append(bl)
        data.append(row)
    mat = pd.DataFrame(data, index=rows, columns=columns, dtype=object)

    return mat, rq_lookup, ct_lookup, bl_lookup


def _extract_a_number(text):
    if not isinstance(text, str):
        return None
    m = re.search(r"(A\d{4})", text)
    return m.group(1) if m else None


# E& acceptance criteria — derived from the Blocked Devices sheet legend
ACCEPTABLE_GRADES = {"A+", "A-Plus", "A", "B", "Grade A+", "Grade A-Plus",
                     "Grade A", "Grade B"}
MIN_BATTERY = 85.0
ACCEPTABLE_A_NUMBERS = set(A_NUMBER_FAMILY.keys())


def evaluate_device(grade, battery, a_number, mdm, apple_id, working):
    """Apply the full E& checklist to one device. Returns (eligible, [reasons])."""
    reasons = []

    # Grade
    if grade is None or (isinstance(grade, float) and pd.isna(grade)):
        reasons.append("Grade missing")
    elif str(grade).strip() not in ACCEPTABLE_GRADES:
        reasons.append(f"Grade {grade}")

    # Battery
    try:
        b = float(battery)
        if b < MIN_BATTERY:
            reasons.append(f"Battery {b:.0f}%")
    except (TypeError, ValueError):
        reasons.append("Battery missing")

    # A-number
    if a_number is None:
        reasons.append("A-number missing")
    elif a_number not in ACCEPTABLE_A_NUMBERS:
        reasons.append(f"A# {a_number}")

    # MDM
    if isinstance(mdm, str) and mdm.strip() and mdm.strip().lower() not in ("off", "none", "nan"):
        reasons.append(f"MDM {mdm}")

    # Apple ID
    if isinstance(apple_id, str) and apple_id.strip() and apple_id.strip().lower() not in ("off", "none", "nan"):
        reasons.append(f"AppleID present")

    # 100% Working
    if isinstance(working, str) and working.strip().lower() in ("no", "false"):
        reasons.append("Not 100% working")

    return (len(reasons) == 0, reasons)


def build_eligible_candidates(po_df, packed_df, blocked_df,
                              master, phone_check, stack):
    """Scan Phone Check (truth source) for iPhone 15 devices that match an
    in-scope SKU, are NOT already in Packed/Blocked, and pass the E& criteria.

    Returns (df_candidates, df_near_miss) — the second is devices that match an
    SKU but fail one or more criteria; useful as a short-list for re-grading.
    """
    if phone_check is None:
        return None, None

    pc = phone_check.copy()
    # Normalise device attrs
    pc["Family"] = pc["Model"].apply(canon_family)
    pc["StorageC"] = pc["Memory"].apply(canon_storage)
    pc["ColourC"] = pc["Color"].apply(canon_colour)
    pc["A_number"] = pc.get("Regulatory Model Number", pd.Series([None] * len(pc))).apply(_extract_a_number)
    pc["SKU Key"] = list(zip(pc["Family"], pc["StorageC"], pc["ColourC"]))

    valid_skus = set(po_df["SKU Key"])
    excluded = set(packed_df["IMEI"].astype(str)) | set(blocked_df["IMEI"].astype(str))

    # Keep only in-scope SKUs, not already packed/blocked
    pc = pc[pc["SKU Key"].isin(valid_skus)].copy()
    pc = pc[~pc["IMEI"].astype(str).isin(excluded)].copy()

    rows = []
    for _, c in pc.iterrows():
        ok, reasons = evaluate_device(
            grade=c.get("Grade"),
            battery=c.get("Battery Health Percentage"),
            a_number=c.get("A_number"),
            mdm=c.get("MDM Status"),
            apple_id=c.get("AppleID"),
            working=c.get("100% Working"),
        )
        imei = str(c["IMEI"])
        # Enrichment from master + stack
        loc = bin_ = room = stack_id = None
        if master is not None and imei in master["IMEI"].values:
            mrow = master.loc[master["IMEI"] == imei].iloc[0]
            loc = mrow.get("Location")
            bin_ = mrow.get("Bin")
            room = mrow.get("Room")
            stack_id = mrow.get("Stack")
        deal = assessed = None
        if stack is not None and imei in stack["IMEI Number"].values:
            srow = stack.loc[stack["IMEI Number"] == imei].iloc[0]
            deal = srow.get("Appraisal")
            assessed = srow.get("Latest Assessed Grade")

        sku_label = f"{c['Family']} · {c['StorageC']} · {c['ColourC']}"
        rows.append({
            "IMEI": imei,
            "SKU (Model+Storage+Colour)": sku_label,

            # --- Criteria columns (thresholds shown in headers) ---
            "A# (A3090/94/02/06)": c.get("A_number"),
            "Grade (A+/A/B)": c.get("Grade"),
            "Battery (≥85%)": c.get("Battery Health Percentage"),
            "Storage": c["StorageC"],
            "Colour": c["ColourC"],

            # --- Other detail ---
            "MDM": c.get("MDM Status"),
            "AppleID": c.get("AppleID"),
            "100% Working": c.get("100% Working"),
            "Room": room,
            "Bin": bin_,
            "Location": loc,
            "Stack": stack_id,
            "Deal Id": deal,
            "Assessed Grade": assessed,
            "Eligible": "✅" if ok else "⚠️",
            "Fail Reasons": "; ".join(reasons),
        })

    df_all = pd.DataFrame(rows)
    if df_all.empty:
        return df_all, df_all
    # Keep Family as a hidden helper column for downstream filtering/rollup
    df_all["_Family"] = df_all["SKU (Model+Storage+Colour)"].apply(
        lambda s: s.split(" · ")[0] if isinstance(s, str) else None
    )
    df_eligible = df_all[df_all["Eligible"] == "✅"].copy()
    df_near_miss = df_all[df_all["Eligible"] == "⚠️"].copy()
    return df_eligible, df_near_miss


def build_eligibility_summary(po_df, packed_df, df_eligible):
    """Per-SKU rollup combining what's packed vs what's newly eligible."""
    rq_lookup = {sku: int(qty) for sku, qty in zip(po_df["SKU Key"], po_df["Qty"])}
    ct_lookup = Counter(packed_df["SKU Key"].tolist())
    if df_eligible is None or df_eligible.empty:
        el_lookup = Counter()
    else:
        el_lookup = Counter(zip(
            df_eligible["_Family"], df_eligible["Storage"], df_eligible["Colour"]
        ))

    rows = []
    for _, r in po_df.iterrows():
        sku = r["SKU Key"]
        req = int(r["Qty"])
        packed = ct_lookup.get(sku, 0)
        eligible = el_lookup.get(sku, 0) if isinstance(el_lookup, Counter) else 0
        gap = max(req - packed, 0)
        will_close = min(eligible, gap)
        rows.append({
            "Line #": r["Line #"],
            "Model": r["Family"],
            "Storage": r["Storage"],
            "Colour": r["Colour"],
            "Required": req,
            "Packed": packed,
            "Gap": gap,
            "Eligible (new)": eligible,
            "Could Fill": will_close,
            "Still Short": max(gap - eligible, 0),
        })
    return pd.DataFrame(rows)


def build_model_summary(po_df, packed_df, blocked_df):
    rows = []
    for family in ["iPhone 15", "iPhone 15 Plus", "iPhone 15 Pro", "iPhone 15 Pro Max"]:
        req = int(po_df.loc[po_df["Family"] == family, "Qty"].sum())
        packed = int((packed_df["Family"] == family).sum())
        blocked = int((blocked_df["Family"] == family).sum())
        remaining = max(req - packed, 0)
        rows.append({
            "Model": family,
            "Required": req,
            "Packed": packed,
            "Blocked": blocked,
            "Remaining": remaining,
            "% Complete": f"{(packed / req * 100):.1f}%" if req else "-",
        })
    df = pd.DataFrame(rows)
    total = {
        "Model": "TOTAL",
        "Required": df["Required"].sum(),
        "Packed": df["Packed"].sum(),
        "Blocked": df["Blocked"].sum(),
        "Remaining": df["Remaining"].sum(),
        "% Complete": (
            f"{(df['Packed'].sum() / df['Required'].sum() * 100):.1f}%"
            if df["Required"].sum() else "-"
        ),
    }
    df = pd.concat([df, pd.DataFrame([total])], ignore_index=True)
    return df


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="E& Fulfillment Dashboard",
    page_icon="📦",
    layout="wide",
)

st.title("📦 E& / Etisalat — PO Fulfillment Dashboard")
st.caption(
    "Auto-fills the E& Order Overview matrix from uploaded files. "
    "No manual entry — every cell is computed from the source workbooks."
)

with st.sidebar:
    st.header("Upload Files")
    st.markdown(
        "**1. E& Project Guidelines** *(required — contains PO, Packed, Blocked sheets)*"
    )
    eand_file = st.file_uploader("E& Project Guidelines.xlsx", type=["xlsx"], key="eand")

    st.markdown("**2. Master Template** *(optional — adds Location)*")
    master_file = st.file_uploader("Master Template.xlsx", type=["xlsx"], key="master")

    st.markdown("**3. Phone Check Bulk Lookup** *(optional — adds Battery %, Grade)*")
    pc_file = st.file_uploader("bulk_lookup_data.xlsx", type=["xlsx"], key="pc")

    st.markdown("**4. Stack Bulk Upload** *(optional — adds Stack/Dealer info)*")
    stack_file = st.file_uploader("Stack Bulk Upload.xlsx", type=["xlsx"], key="stack")

    st.divider()
    st.caption(
        "Matching rules: case- & spacing-insensitive on Model, Storage, Colour. "
        "CT and BL counts are read from the uploaded E& workbook's "
        "*Packed Devices Details* and *Blocked Devices* sheets."
    )

if not eand_file:
    st.info("⬅️  Upload the **E& Project Guidelines** workbook to begin.")
    st.stop()

# --- Parse E& workbook ---
try:
    eand_bytes = eand_file.getvalue()
    sheets = _read_all_sheets(eand_bytes)
except Exception as exc:
    st.error(f"Failed to read E& workbook: {exc}")
    st.stop()

required_sheets = ["PO", "Packed Devices Details", "Blocked Devices"]
missing = [s for s in required_sheets if s not in sheets]
if missing:
    st.error(f"E& workbook is missing required sheet(s): {missing}")
    st.stop()

po_df = parse_po_sheet(sheets["PO"])
packed_df = parse_packed_sheet(sheets["Packed Devices Details"])
blocked_df = parse_blocked_sheet(sheets["Blocked Devices"])

po_overview = parse_po_overview(sheets.get("PO Overview", pd.DataFrame()))
checklist_df = (
    parse_checklist_sheet(sheets["Packing & Dispatch Checklist"])
    if "Packing & Dispatch Checklist" in sheets else None
)
material_df = (
    parse_material_sheet(sheets["Material "])
    if "Material " in sheets else
    (parse_material_sheet(sheets["Material"]) if "Material" in sheets else None)
)

# --- Parse dynamic files (optional) ---
master = None
if master_file is not None:
    try:
        mraw = _read_sheet(master_file.getvalue(), sheet_name="StockTake Template", header=0)
        master = parse_master(mraw)
    except Exception as exc:
        st.warning(f"Could not parse Master Template: {exc}")

phone_check = None
if pc_file is not None:
    try:
        praw = _read_sheet(pc_file.getvalue(), sheet_name="Sheet1", header=0)
        phone_check = parse_phone_check(praw)
    except Exception as exc:
        st.warning(f"Could not parse Phone Check: {exc}")

stack = None
if stack_file is not None:
    try:
        sraw = _read_sheet(stack_file.getvalue(), sheet_name="BulkSell", header=0)
        stack = parse_stack_bulk(sraw)
    except Exception as exc:
        st.warning(f"Could not parse Stack Bulk Upload: {exc}")

# --- Conflict check (Doubt 5: device cannot be in both Packed and Blocked) ---
conflict_imeis = set(packed_df["IMEI"]) & set(blocked_df["IMEI"])
if conflict_imeis:
    st.warning(
        f"⚠️ {len(conflict_imeis)} IMEI(s) appear in BOTH Packed and Blocked. "
        f"This should not happen. First few: {sorted(conflict_imeis)[:5]}"
    )

# --- Build matrix & summary ---
matrix, rq_lookup, ct_lookup, bl_lookup = build_overview_matrix(po_df, packed_df, blocked_df)
summary = build_model_summary(po_df, packed_df, blocked_df)

# --- KPIs ---
total_required = int(po_df["Qty"].sum())
total_packed = len(packed_df)
total_blocked = len(blocked_df)
total_remaining = max(total_required - total_packed, 0)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("PO Number", po_overview.get("PO Number", "-"))
k2.metric("Total Required", f"{total_required}")
k3.metric("Packed", f"{total_packed}", delta=f"{total_packed - total_required}")
k4.metric("Blocked", f"{total_blocked}")
k5.metric("Remaining", f"{total_remaining}")

# --- Tabs ---
tab_summary, tab_drill, tab_eligible = st.tabs(
    ["📈 Status by Model",
     "🔍 SKU Drill-down",
     "🟢 Eligible Candidates"]
)

# === Tab 1: Model summary ===
with tab_summary:
    st.subheader("Fulfillment Status by Model")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("**SKU Status (all 35 SKUs)**")

    sku_rows = []
    for _, r in po_df.iterrows():
        sku = r["SKU Key"]
        ct = ct_lookup.get(sku, 0)
        bl = bl_lookup.get(sku, 0)
        req = int(r["Qty"])
        sku_rows.append({
            "Line #": r["Line #"],
            "Item Code": r["Item Code"],
            "Model": r["Family"],
            "Storage": r["Storage"],
            "Colour": r["Colour"],
            "Required": req,
            "Packed": ct,
            "Blocked": bl,
            "Remaining": max(req - ct, 0),
            "Status": "✅ Complete" if ct >= req else ("🚧 In Progress" if ct > 0 else "⬜ Not Started"),
        })
    sku_df = pd.DataFrame(sku_rows)
    st.dataframe(sku_df, use_container_width=True, hide_index=True)

# === Tab 3: SKU drill-down ===
with tab_drill:
    st.subheader("Per-SKU Drill-down")
    options = [
        f"{r['Line #']:02d} · {r['Family']} {r['Storage']} {r['Colour']}"
        for _, r in po_df.iterrows()
    ]
    pick = st.selectbox("Select SKU", options)
    line_no = int(pick.split(" · ")[0])
    row = po_df[po_df["Line #"] == line_no].iloc[0]
    sku = row["SKU Key"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Required", int(row["Qty"]))
    c2.metric("Packed", ct_lookup.get(sku, 0))
    c3.metric("Blocked", bl_lookup.get(sku, 0))
    c4.metric("Remaining", max(int(row["Qty"]) - ct_lookup.get(sku, 0), 0))

    st.markdown("#### ✅ Packed Devices")
    packed_match = packed_df[packed_df["SKU Key"] == sku].copy()
    if packed_match.empty:
        st.info("No packed devices for this SKU yet.")
    else:
        rows = []
        for _, p in packed_match.iterrows():
            enrich = enrich_device_row(p["IMEI"], master, phone_check, stack)
            rows.append({
                "IMEI": p["IMEI"],
                "Deal Id": p.get("Deal Id"),
                "A Number": enrich["A-number"] or p.get("A Number"),
                "Location": enrich["Location"],
                "Bin": enrich["Bin"],
                "Stack": enrich["Stack"],
                "Battery %": enrich["Battery %"],
                "PC Grade (truth)": enrich["PC Grade"],
                "100% Working": enrich["100% Working"],
                "MDM": enrich["MDM Status"],
                "AppleID": enrich["AppleID"],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.markdown("#### 🚫 Blocked Devices")
    blocked_match = blocked_df[blocked_df["SKU Key"] == sku].copy()
    if blocked_match.empty:
        st.info("No blocked devices for this SKU.")
    else:
        rows = []
        for _, b in blocked_match.iterrows():
            enrich = enrich_device_row(b["IMEI"], master, phone_check, stack)
            # Reason inference: compare phone-check truth against E& criteria
            reasons = []
            pc_grade = enrich["PC Grade"]
            if pc_grade and pc_grade not in ("A+", "A-Plus", "A", "B"):
                reasons.append(f"Grade {pc_grade}")
            batt = enrich["Battery %"]
            try:
                if batt is not None and float(batt) < 85:
                    reasons.append(f"Battery {batt}%")
            except (ValueError, TypeError):
                pass
            mdm = enrich["MDM Status"]
            if mdm and str(mdm).lower() not in ("off", "nan", "none", ""):
                reasons.append(f"MDM {mdm}")
            a_no = enrich["A-number"] or b.get("A number")
            if a_no and a_no not in A_NUMBER_FAMILY:
                reasons.append(f"A# {a_no}")
            if not reasons:
                reasons.append(b.get("Status") or "—")
            rows.append({
                "IMEI": b["IMEI"],
                "Deal Id": b.get("Deal Id"),
                "A Number": a_no,
                "Location": enrich["Location"],
                "Battery %": batt,
                "PC Grade (truth)": pc_grade,
                "100% Working": enrich["100% Working"],
                "MDM": mdm,
                "Block Reason(s)": ", ".join(reasons),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# === Tab 4: Eligible Candidates ===
with tab_eligible:
    st.subheader("🟢 Eligible Candidates — auto-discovered from uploaded data")
    st.caption(
        "Scans the Phone Check file (source of truth) for iPhone 15 devices that "
        "match an in-scope SKU, are not already in Packed/Blocked, and pass the "
        "full E& checklist (Grade ∈ {A+, A, B}, Battery ≥ 85%, A-number ∈ "
        "{A3090, A3094, A3102, A3106}, MDM off, no Apple ID). Master adds the "
        "physical Location; Stack adds Deal Id / Assessed Grade."
    )

    if phone_check is None:
        st.info("Upload the **Phone Check Bulk Lookup** file in the sidebar to compute eligible candidates.")
    else:
        df_eligible, df_near = build_eligible_candidates(
            po_df, packed_df, blocked_df, master, phone_check, stack
        )

        if df_eligible is None or (df_eligible.empty and df_near.empty):
            st.success(
                "No iPhone 15 devices in the Phone Check file match an in-scope SKU "
                "outside of what's already packed/blocked."
            )
        else:
            n_eligible = len(df_eligible)
            n_near = len(df_near)
            elig_summary = build_eligibility_summary(po_df, packed_df, df_eligible)
            could_fill = int(elig_summary["Could Fill"].sum())
            total_gap = int(elig_summary["Gap"].sum())

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Fully Eligible", n_eligible)
            c2.metric("Near-Miss (1+ fail)", n_near)
            c3.metric("Outstanding Gap", total_gap)
            c4.metric("Could Be Filled", could_fill,
                      delta=f"-{total_gap - could_fill} still short" if total_gap else None)

            st.markdown("#### Per-SKU Eligibility Rollup")
            st.dataframe(
                elig_summary,
                use_container_width=True, hide_index=True,
                column_config={
                    "Could Fill": st.column_config.NumberColumn(format="%d", help="min(Gap, Eligible)"),
                    "Still Short": st.column_config.NumberColumn(format="%d"),
                },
            )

            # SKU filter
            st.markdown("#### Candidate Devices")
            sku_col = "SKU (Model+Storage+Colour)"
            sku_options = ["All SKUs"] + sorted(df_eligible[sku_col].unique().tolist())
            picked = st.selectbox("Filter by SKU", sku_options, key="eligible_sku")

            view_eligible = df_eligible.copy()
            view_near = df_near.copy()
            if picked != "All SKUs":
                view_eligible = view_eligible[view_eligible[sku_col] == picked]
                view_near = view_near[view_near[sku_col] == picked]

            st.markdown(f"##### ✅ Fully Eligible ({len(view_eligible)})")
            if view_eligible.empty:
                st.info("No fully-eligible candidates for this filter.")
            else:
                st.dataframe(
                    view_eligible.drop(columns=["Eligible", "Fail Reasons", "_Family"]),
                    use_container_width=True, hide_index=True,
                )
                st.download_button(
                    "⬇️ Download eligible IMEIs (CSV)",
                    view_eligible.to_csv(index=False).encode("utf-8"),
                    file_name="eligible_candidates.csv",
                    mime="text/csv",
                )

            with st.expander(f"⚠️ Near-Miss — devices failing 1+ criterion ({len(view_near)})"):
                if view_near.empty:
                    st.caption("Nothing here.")
                else:
                    st.dataframe(
                        view_near.drop(columns=["Eligible", "_Family"]),
                        use_container_width=True, hide_index=True,
                    )

