
# app.py — SQA India Digital Obeya Dashboard
# Tech Stack: Python + Streamlit + SQLite
# Design: Ported from React/Tailwind specification

import os
import sqlite3
import uuid
import pandas as pd
import io
import json
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import streamlit as st

# =========================
# CONFIG / CONSTANTS
# =========================

APP_TITLE = "SQ&D India Obeya Dashboard"
DB_FILE = "feb2025_obeyadb.db" 
SCHEMA_VERSION = "2.5"

BRANDS_DEFAULT = ["QUESTER", "CRONER", "QUON"]
SEVERITY_OPTIONS = ["0P", "5P", "25P", "100P"]
ACTION_HEALTH_OPTIONS = ["On Track", "No Targets", "Delayed"]
YESNO = ["No", "Yes"]

ALL_WEEKS = [f"WK{i:02d}" for i in range(1, 54)]

# =========================
# UTILITIES
# =========================

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def week_label(n: int) -> str:
    return f"WK{int(n):02d}"

def current_iso_week() -> int:
    return date.today().isocalendar().week

def get_week_number(week_str: str) -> int:
    try:
        if not week_str: return 0
        return int(week_str.upper().replace("WK", ""))
    except:
        return 0

def get_query_params() -> Dict[str, List[str]]:
    if hasattr(st, "query_params"):
        qp = dict(st.query_params)
        return {k: [v] if not isinstance(v, list) else v for k, v in qp.items()}
    return {}

def set_query_params(**kwargs):
    clean = {k: v for k, v in kwargs.items() if v is not None and v != ""}
    if hasattr(st, "query_params"):
        st.query_params.clear()
        for k, v in clean.items():
            st.query_params[k] = v

def rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

def html_escape(s: str) -> str:
    if not s: return ""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

def parse_date_str(d_str: str) -> Optional[date]:
    if not d_str: return None
    try:
        return datetime.strptime(d_str, '%Y-%m-%d').date()
    except:
        return None

# =========================
# DATA MODEL
# =========================

@dataclass
class Issue:
    id: str
    brand: str
    week_str: str
    owner_initials: str
    sqe_name: str
    issue_info: str

    supplier_sqa: str
    fkf_week: str
    part_number: str
    repeat_issue: str
    severity: str
    quantity: str
    ng_rtv: str

    ftt_impact: str
    field_impact: str
    other_sites_impact: str

    action_health: str
    actions_completed: int

    d3_target: str; d3_status: str; d3_remarks: str
    d5_target: str; d5_status: str; d5_remarks: str
    d8_target: str; d8_status: str; d8_remarks: str
    
    status: str
    created_at: str
    updated_at: str
    closed_at: str

    @property
    def week_num(self) -> int:
        return get_week_number(self.week_str)


# =========================
# DB LAYER
# =========================

def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS issues (
        id TEXT PRIMARY KEY,
        brand TEXT NOT NULL,
        week_str TEXT NOT NULL,
        owner_initials TEXT NOT NULL,
        sqe_name TEXT,
        issue_info TEXT NOT NULL,

        supplier_sqa TEXT,
        fkf_week TEXT,
        part_number TEXT,
        repeat_issue TEXT,
        severity TEXT,
        quantity TEXT,
        ng_rtv TEXT,

        ftt_impact TEXT,
        field_impact TEXT,
        other_sites_impact TEXT,

        action_health TEXT,
        actions_completed INTEGER DEFAULT 0,

        d3_target TEXT, d3_status TEXT, d3_remarks TEXT,
        d5_target TEXT, d5_status TEXT, d5_remarks TEXT,
        d8_target TEXT, d8_status TEXT, d8_remarks TEXT,

        status TEXT DEFAULT 'OPEN',
        created_at TEXT,
        updated_at TEXT,
        closed_at TEXT
    )
    """)
    # Table for storing the data editor json blobs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS metrics (
        key TEXT PRIMARY KEY,
        data TEXT
    )
    """)
    conn.commit()

    # --- MIGRATION: Auto-add sqe_name if missing (prevents crash on old DBs) ---
    try:
        cur.execute("SELECT sqe_name FROM issues LIMIT 1")
    except sqlite3.OperationalError:
        try:
            cur.execute("ALTER TABLE issues ADD COLUMN sqe_name TEXT")
            conn.commit()
        except Exception:
            pass

def hard_reset_db(conn: sqlite3.Connection):
    # DROP tables instead of deleting file to avoid "file locked" errors
    conn.execute("DROP TABLE IF EXISTS issues")
    conn.execute("DROP TABLE IF EXISTS metrics")
    conn.commit()
    init_db(conn)

def create_issue(conn: sqlite3.Connection, data: Dict) -> str:
    issue_id = str(uuid.uuid4())
    ts = now_ts()
    
    keys = [
        "id", "brand", "week_str", "owner_initials", "sqe_name", "issue_info",
        "supplier_sqa", "fkf_week", "part_number", "repeat_issue", "severity",
        "quantity", "ng_rtv", "ftt_impact", "field_impact", "other_sites_impact",
        "action_health", "actions_completed", 
        "d3_target", "d3_status", "d3_remarks",
        "d5_target", "d5_status", "d5_remarks",
        "d8_target", "d8_status", "d8_remarks",
        "status", "created_at", "updated_at"
    ]
    
    vals = [issue_id]
    vals.append(data.get("brand"))
    vals.append(data.get("week_str"))
    vals.append(data.get("owner_initials"))
    vals.append(data.get("sqe_name", ""))
    vals.append(data.get("issue_info"))
    
    vals.append(data.get("supplier_sqa", ""))
    vals.append(data.get("fkf_week", ""))
    vals.append(data.get("part_number", ""))
    vals.append(data.get("repeat_issue", "No"))
    vals.append(data.get("severity", "5P"))
    vals.append(data.get("quantity", ""))
    vals.append(data.get("ng_rtv", ""))
    
    vals.append(data.get("ftt_impact", "No"))
    vals.append(data.get("field_impact", "No"))
    vals.append(data.get("other_sites_impact", "No"))
    
    vals.append(data.get("action_health", "No Targets"))
    vals.append(data.get("actions_completed", 0))
    
    vals.append(data.get("d3_target", ""))
    vals.append(data.get("d3_status", ""))
    vals.append(data.get("d3_remarks", ""))
    vals.append(data.get("d5_target", ""))
    vals.append(data.get("d5_status", ""))
    vals.append(data.get("d5_remarks", ""))
    vals.append(data.get("d8_target", ""))
    vals.append(data.get("d8_status", ""))
    vals.append(data.get("d8_remarks", ""))
    
    vals.append("OPEN")
    vals.append(ts)
    vals.append(ts)

    q = f"INSERT INTO issues ({','.join(keys)}) VALUES ({','.join(['?']*len(keys))})"
    conn.execute(q, vals)
    conn.commit()
    return issue_id

def update_issue(conn: sqlite3.Connection, issue_id: str, data: Dict):
    ts = now_ts()
    set_parts = []
    vals = []
    for k, v in data.items():
        set_parts.append(f"{k}=?")
        vals.append(v)
    set_parts.append("updated_at=?")
    vals.append(ts)
    vals.append(issue_id)

    conn.execute(f"UPDATE issues SET {', '.join(set_parts)} WHERE id=?", vals)
    conn.commit()

def close_issue(conn: sqlite3.Connection, issue_id: str):
    ts = now_ts()
    conn.execute(
        "UPDATE issues SET status='CLOSED', closed_at=?, updated_at=? WHERE id=?",
        (ts, ts, issue_id)
    )
    conn.commit()

def fetch_issues(conn: sqlite3.Connection) -> List[Issue]:
    rows = conn.execute("SELECT * FROM issues ORDER BY status DESC, brand, week_str").fetchall()
    return [row_to_issue(r) for r in rows]

def fetch_issue_by_id(conn: sqlite3.Connection, issue_id: str) -> Optional[Issue]:
    r = conn.execute("SELECT * FROM issues WHERE id=?", (issue_id,)).fetchone()
    return row_to_issue(r) if r else None

def row_to_issue(r: sqlite3.Row) -> Issue:
    # Safely retrieve value, handling None/NULL from DB
    def get_val(key, default=""):
        try: 
            val = r[key]
            return val if val is not None else default
        except: 
            return default

    return Issue(
        id=r["id"],
        brand=r["brand"],
        week_str=get_val("week_str", ""),
        owner_initials=r["owner_initials"],
        sqe_name=get_val("sqe_name", ""),
        issue_info=r["issue_info"],
        supplier_sqa=get_val("supplier_sqa"),
        fkf_week=get_val("fkf_week"),
        part_number=get_val("part_number"),
        repeat_issue=get_val("repeat_issue") or "No",
        severity=get_val("severity") or "0P",
        quantity=get_val("quantity"),
        ng_rtv=get_val("ng_rtv"),
        ftt_impact=get_val("ftt_impact") or "No",
        field_impact=get_val("field_impact") or "No",
        other_sites_impact=get_val("other_sites_impact") or "No",
        action_health=get_val("action_health") or "No Targets",
        actions_completed=int(get_val("actions_completed") or 0),
        
        d3_target=get_val("d3_target"), d3_status=get_val("d3_status"), d3_remarks=get_val("d3_remarks"),
        d5_target=get_val("d5_target"), d5_status=get_val("d5_status"), d5_remarks=get_val("d5_remarks"),
        d8_target=get_val("d8_target"), d8_status=get_val("d8_status"), d8_remarks=get_val("d8_remarks"),
        
        status=get_val("status") or "OPEN",
        created_at=get_val("created_at"),
        updated_at=get_val("updated_at"),
        closed_at=get_val("closed_at"),
    )

def export_to_excel(conn: sqlite3.Connection):
    try:
        df = pd.read_sql_query("SELECT * FROM issues", conn)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Obeya Issues')
        return buffer.getvalue()
    except Exception as e:
        return None

# =========================
# METRICS / TABLE UTILS
# =========================
def load_metric_df(conn: sqlite3.Connection, key: str, default_data: dict) -> pd.DataFrame:
    row = conn.execute("SELECT data FROM metrics WHERE key=?", (key,)).fetchone()
    if row and row['data']:
        try:
            return pd.read_json(io.StringIO(row['data']))
        except:
            pass
    return pd.DataFrame(default_data)

def save_metric_df(conn: sqlite3.Connection, key: str, df: pd.DataFrame):
    json_str = df.to_json()
    conn.execute("INSERT OR REPLACE INTO metrics (key, data) VALUES (?, ?)", (key, json_str))
    conn.commit()


# =========================
# BUSINESS RULES (Visuals)
# =========================

def chip_bg_class(issue: Issue) -> str:
    sev = (issue.severity or "").upper().strip()
    ftt = (issue.ftt_impact or "No")
    field = (issue.field_impact or "No")

    if ftt == "Yes" and sev in ("25P", "100P"):
        return "chip-red"
    if ftt == "Yes" and sev in ("0P", "5P"):
        return "chip-orange"
    if field == "Yes":
        return "chip-yellow"
    return "chip-neutral"

def dot_class(action_health: str) -> str:
    ah = (action_health or "").strip()
    if ah == "On Track":
        return "dot-green"
    if ah == "Delayed":
        return "dot-red"
    return "dot-yellow"

# =========================
# UI: STYLES
# =========================

def inject_css():
    st.markdown(
        """
<style>
/* Reset & Base */
.block-container { padding-top: 1rem; padding-bottom: 2rem; max-width: 100%; padding-left: 1rem; padding-right: 1rem; }
html, body { font-family: 'Inter', sans-serif; background-color: #f9fafb; color: #1e293b; font-size: 16px; }

/* Obeya Table Structure */
.board-container {
    overflow-x: auto;
    background: #f3f4f6;
    padding: 1rem;
    border-radius: 0.5rem;
}

.obeya-table {
    width: 100%;
    min-width: 1400px;
    border-collapse: separate;
    border-spacing: 0;
    background: white;
    border: 1px solid #d1d5db;
    border-radius: 0.5rem;
    /* overflow: hidden;  <-- REMOVED to allow tooltips to pop out */
}

/* Headers - Increased Font */
.obeya-th {
    padding: 1rem;
    font-size: 1.1rem;
    font-weight: 700;
    text-align: center;
    border-right: 1px solid #475569;
    border-bottom: 1px solid #1e293b;
    background-color: #1e293b;
    color: white;
    position: sticky;
    top: 0;
    z-index: 10;
}
.obeya-th.pending { background-color: #7c2d12; color: #fed7aa; }
.obeya-th.closed { background-color: #14532d; color: #bbf7d0; }
.obeya-th.sticky-col { left: 0; z-index: 20; width: 110px; }

/* Body Rows */
.obeya-rowhead {
    position: sticky;
    left: 0;
    z-index: 10;
    background-color: #f1f5f9;
    color: #334155;
    font-size: 1.1rem;
    font-weight: 800;
    text-align: center;
    border-right: 1px solid #d1d5db;
    border-bottom: 1px solid #e5e7eb;
    padding: 0.5rem;
    width: 110px;
    box-shadow: 4px 0 5px -2px rgba(0,0,0,0.1);
}

.obeya-td {
    vertical-align: top;
    padding: 0.35rem;
    border-right: 1px solid #e5e7eb;
    border-bottom: 1px solid #e5e7eb;
    height: 180px; /* Taller */
    width: 170px; /* Wider */
    background-color: white;
}
.obeya-td.bg-amber { background-color: #fffbeb; }
.obeya-td.bg-green { background-color: #f0fdf4; }

/* Grid Layouts */
.cell-grid {
    display: grid;
    gap: 0.35rem;
    width: 100%;
    height: 100%;
}
.grid-2 { grid-template-columns: repeat(2, 1fr); grid-template-rows: repeat(2, 1fr); }
.grid-3 { grid-template-columns: repeat(3, 1fr); grid-template-rows: repeat(3, 1fr); }

/* Chip Styles - Increased Font */
.chip {
    display: flex;
    align-items: center;
    justify-content: center;
    position: relative;
    border-radius: 0.35rem;
    border-width: 1px;
    border-style: solid;
    cursor: pointer;
    text-decoration: none !important;
    transition: all 0.2s;
    width: 100%;
    height: 100%;
    color: #1e293b !important;
    font-weight: 800;
    font-size: 1.05rem; /* Bigger font */
    user-select: none;
}
.chip:hover { transform: scale(1.02); z-index: 5; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }

.chip-red { background-color: #fecaca; border-color: #fca5a5; }
.chip-red:hover { background-color: #fca5a5; }
.chip-orange { background-color: #fed7aa; border-color: #fdba74; }
.chip-orange:hover { background-color: #fdba74; }
.chip-yellow { background-color: #fef08a; border-color: #fde047; }
.chip-yellow:hover { background-color: #fde047; }
.chip-neutral { background-color: #f3f4f6; border-color: #d1d5db; }
.chip-neutral:hover { background-color: #e5e7eb; }

/* Dots */
.dot {
    position: absolute;
    bottom: 3px;
    right: 3px;
    width: 10px;
    height: 10px;
    border-radius: 9999px;
    border: 1px solid white;
}
.dot-green { background-color: #22c55e; }
.dot-yellow { background-color: #eab308; }
.dot-red { background-color: #dc2626; }

/* Custom Tooltip CSS - Updated for better readability */
.tooltip-container {
    position: relative;
    width: 100%;
    height: 100%;
}
.tooltip-text {
    visibility: hidden;
    width: 240px;
    background-color: #ffffff;
    color: #0f172a;
    text-align: left;
    border-radius: 8px;
    padding: 12px;
    position: absolute;
    z-index: 100;
    bottom: 110%;
    left: 50%;
    transform: translateX(-50%);
    opacity: 0;
    transition: opacity 0.2s;
    font-size: 1rem;
    font-weight: 500;
    pointer-events: none;
    line-height: 1.5;
    box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
    border: 1px solid #e2e8f0;
}
.tooltip-text::after {
  content: "";
  position: absolute;
  top: 100%;
  left: 50%;
  margin-left: -5px;
  border-width: 5px;
  border-style: solid;
  border-color: #ffffff transparent transparent transparent;
}
.tooltip-container:hover .tooltip-text {
    visibility: visible;
    opacity: 1;
}

/* Metric Headers */
.metric-header {
    font-size: 1.4rem;
    font-weight: 800;
    color: #334155;
    margin-bottom: 0.5rem;
    border-bottom: 3px solid #cbd5e1;
    padding-bottom: 0.25rem;
}

/* Form Styles for Compactness */
.stTextInput, .stSelectbox, .stDateInput, .stTextArea {
    margin-bottom: 0px !important;
}
div[data-testid="stForm"] {
    padding: 1rem;
    border-radius: 8px;
    border: 1px solid #e5e7eb;
}
</style>
        """,
        unsafe_allow_html=True,
    )

# =========================
# UI COMPONENTS
# =========================

def render_add_issue_section(conn: sqlite3.Connection, visible_weeks: List[str]):
    # Use session state to toggle visibility
    if "show_add_issue" not in st.session_state:
        st.session_state.show_add_issue = False

    col_btn, _ = st.columns([2, 10])
    with col_btn:
        if st.button("➕ Add Issue", use_container_width=True):
            st.session_state.show_add_issue = not st.session_state.show_add_issue

    if st.session_state.show_add_issue:
        st.markdown("#### Publish New Issue")
        with st.form("add_issue_form", clear_on_submit=True):
            col1, col2, col3, col4 = st.columns([1.5, 1, 1.5, 2.5]) # Adjusted cols to fit SQE Name
            
            with col1:
                brand = st.selectbox("Brand", BRANDS_DEFAULT)
            with col2:
                # Use ALL_WEEKS but select current
                week = st.selectbox("Week", ALL_WEEKS, index=ALL_WEEKS.index(visible_weeks[0]) if visible_weeks else 0)
            with col3:
                c_own, c_sqe = st.columns(2)
                with c_own:
                    owner = st.text_input("Initials", placeholder="INT", max_chars=3)
                with c_sqe:
                    sqe_name = st.text_input("SQE Name", placeholder="Name")
            with col4:
                info = st.text_input("Issue Info", placeholder="Short description...")

            colA, colB = st.columns([3, 1])
            with colA:
                c1, c2, c3, c4 = st.columns(4)
                with c1: severity = st.selectbox("Severity", SEVERITY_OPTIONS, index=1)
                with c2: supplier = st.text_input("Supplier", placeholder="Optional")
                with c3: ftt = st.checkbox("FTT Impact?")
                with c4: field = st.checkbox("Field Impact?")
            
            with colB:
                submitted = st.form_submit_button("Publish Issue", use_container_width=True, type="primary")

            if submitted:
                if not owner or not info:
                    st.error("Owner Initials and Issue Info are required.")
                else:
                    create_issue(conn, {
                        "brand": brand,
                        "week_str": week,
                        "owner_initials": owner.upper(),
                        "sqe_name": sqe_name,
                        "issue_info": info,
                        "severity": severity,
                        "supplier_sqa": supplier,
                        "ftt_impact": "Yes" if ftt else "No",
                        "field_impact": "Yes" if field else "No"
                    })
                    st.success("Issue Published!")
                    rerun()

def render_board(issues: List[Issue], visible_weeks: List[str]):
    board_data = {brand: {w: [] for w in visible_weeks} for brand in BRANDS_DEFAULT}
    for brand in BRANDS_DEFAULT:
        board_data[brand]["PENDING"] = []
        board_data[brand]["CLOSED"] = []

    min_visible_num = get_week_number(visible_weeks[0]) if visible_weeks else 999

    for i in issues:
        if i.brand not in BRANDS_DEFAULT: continue
        
        if i.status == "CLOSED":
            board_data[i.brand]["CLOSED"].append(i)
        elif i.week_str in visible_weeks:
            board_data[i.brand][i.week_str].append(i)
        elif i.week_num < min_visible_num:
             board_data[i.brand]["PENDING"].append(i)

    # Construct headers without leading spaces
    headers_html = ['<th class="obeya-th sticky-col">MODELS</th>']
    for w in visible_weeks:
        headers_html.append(f'<th class="obeya-th">{w}</th>')
    headers_html.append('<th class="obeya-th pending">PENDING</th>')
    headers_html.append('<th class="obeya-th closed">CLOSED</th>')

    rows_html = []
    for brand in BRANDS_DEFAULT:
        cells = [f'<td class="obeya-rowhead">{brand}</td>']
        for w in visible_weeks:
            cells.append(render_cell(board_data[brand][w], ""))
        cells.append(render_cell(board_data[brand]["PENDING"], "bg-amber"))
        cells.append(render_cell(board_data[brand]["CLOSED"], "bg-green"))
        rows_html.append(f'<tr>{"".join(cells)}</tr>')

    # IMPORTANT: Use a join without newlines and spaces to prevent Markdown code-block triggers
    table_content = "".join(rows_html)
    header_content = "".join(headers_html)
    
    # We build the string flat to ensure no line starts with spaces
    table_html = (
        '<div class="board-container">'
        '<table class="obeya-table">'
        f'<thead><tr>{header_content}</tr></thead>'
        f'<tbody>{table_content}</tbody>'
        '</table>'
        '</div>'
    )
    
    st.markdown(table_html, unsafe_allow_html=True)
def render_cell(issues: List[Issue], bg_class: str) -> str:
    count = len(issues)
    grid_class = "grid-3" if count > 4 else "grid-2"
    
    chips_html = []
    for issue in issues:
        # Tooltip content
        # SQE Name added
        display_name = issue.sqe_name if issue.sqe_name else issue.owner_initials
        tooltip_content = f"<b>SQE: {display_name}</b><br/>Supplier: {issue.supplier_sqa or 'NA'}<br/>{issue.issue_info}"
        safe_initials = html_escape(issue.owner_initials)

        # Custom Tooltip Structure: div.tooltip-container > a.chip > span.tooltip-text
        chip = f"""
        <div class="tooltip-container">
            <a class="chip {chip_bg_class(issue)}" href="?issue_id={issue.id}" target="_self">
                {safe_initials}
                <span class="dot {dot_class(issue.action_health)}"></span>
            </a>
            <span class="tooltip-text">{tooltip_content}</span>
        </div>
        """
        chips_html.append(chip)
    
    inner_html = f'<div class="cell-grid {grid_class}">{"".join(chips_html)}</div>'
    return f'<td class="obeya-td {bg_class}">{inner_html}</td>'

# CHECK FOR DIALOG AVAILABILITY
if hasattr(st, "dialog"):
    dialog_decorator = st.dialog
elif hasattr(st, "experimental_dialog"):
    dialog_decorator = st.experimental_dialog
else:
    # Fallback mock decorator if strictly needed, though st.dialog should exist in modern streamlit
    def dialog_decorator(title, width="medium"):
        def decorator(func):
            def wrapper(*args, **kwargs):
                st.markdown(f"### {title}")
                return func(*args, **kwargs)
            return wrapper
        return decorator

# COMPACT DIALOG - Uses @st.dialog for popup behavior
@dialog_decorator("Edit Issue", width="large")
def edit_issue_dialog(conn: sqlite3.Connection, issue: Issue):
    with st.form("edit_issue_form"):
        # ROW 1: Key Metadata
        c1, c2, c3, c4, c5 = st.columns([2, 1.5, 1.5, 1.5, 2])
        with c1: brand = st.selectbox("Brand", BRANDS_DEFAULT, index=BRANDS_DEFAULT.index(issue.brand))
        with c2: 
            wk_idx = ALL_WEEKS.index(issue.week_str) if issue.week_str in ALL_WEEKS else 0
            week_str = st.selectbox("Week", ALL_WEEKS, index=wk_idx)
        with c3: severity = st.selectbox("Severity", SEVERITY_OPTIONS, index=SEVERITY_OPTIONS.index(issue.severity) if issue.severity in SEVERITY_OPTIONS else 0)
        with c4: owner = st.text_input("Owner", value=issue.owner_initials)
        with c5: sqe_name = st.text_input("SQE Name", value=issue.sqe_name)

        # ROW 2: Description (Slim)
        info = st.text_area("Issue Info", value=issue.issue_info, height=68, label_visibility="visible", placeholder="Issue Description")

        # ROW 3: Secondary Data
        r3c1, r3c2, r3c3, r3c4, r3c5 = st.columns(5)
        with r3c1: supplier = st.text_input("Supplier", value=issue.supplier_sqa)
        with r3c2: part_number = st.text_input("Part No.", value=issue.part_number)
        with r3c3: quantity = st.text_input("Qty", value=issue.quantity)
        with r3c4: fkf = st.text_input("FKF Wk", value=issue.fkf_week)
        with r3c5: repeat = st.selectbox("Repeat?", YESNO, index=YESNO.index(issue.repeat_issue))

        # ROW 4: Impacts & Health
        st.write("") # Spacer
        r4c1, r4c2 = st.columns([3, 1])
        with r4c1:
            cc1, cc2, cc3 = st.columns(3)
            with cc1: ftt = st.checkbox("FTT Impact", value=(issue.ftt_impact=="Yes"))
            with cc2: field = st.checkbox("Field Impact", value=(issue.field_impact=="Yes"))
            with cc3: other = st.checkbox("Other Sites", value=(issue.other_sites_impact=="Yes"))
        with r4c2:
            health = st.selectbox("Action Health", ACTION_HEALTH_OPTIONS, index=ACTION_HEALTH_OPTIONS.index(issue.action_health) if issue.action_health in ACTION_HEALTH_OPTIONS else 1, label_visibility="collapsed")

        # ROW 5: Actions Grid
        st.markdown("---")
        # Custom Compact Grid for Actions
        ac1, ac2, ac3 = st.columns(3)
        
        # D3 Column
        with ac1:
            st.caption("**D3 (Containment)**")
            d3_tgt = st.date_input("D3 Tgt", value=parse_date_str(issue.d3_target), label_visibility="collapsed")
            d3_stat = st.text_input("D3 Stat", value=issue.d3_status, placeholder="Status", label_visibility="collapsed")
            d3_rem = st.text_area("D3 Rem", value=issue.d3_remarks, height=68, placeholder="Remarks", label_visibility="collapsed")

        # D5 Column
        with ac2:
            st.caption("**D5 (Root Cause)**")
            d5_tgt = st.date_input("D5 Tgt", value=parse_date_str(issue.d5_target), label_visibility="collapsed")
            d5_stat = st.text_input("D5 Stat", value=issue.d5_status, placeholder="Status", label_visibility="collapsed")
            d5_rem = st.text_area("D5 Rem", value=issue.d5_remarks, height=68, placeholder="Remarks", label_visibility="collapsed")
        
        # D8 Column
        with ac3:
            st.caption("**D8 (Closure)**")
            d8_tgt = st.date_input("D8 Tgt", value=parse_date_str(issue.d8_target), label_visibility="collapsed")
            d8_stat = st.text_input("D8 Stat", value=issue.d8_status, placeholder="Status", label_visibility="collapsed")
            d8_rem = st.text_area("D8 Rem", value=issue.d8_remarks, height=68, placeholder="Remarks", label_visibility="collapsed")

        st.markdown("---")
        
        # Footer
        fc1, fc2, fc3 = st.columns([1.5, 3, 1.5])
        with fc1:
             completed = st.checkbox("✅ Actions Completed", value=bool(issue.actions_completed))
        with fc2:
             if completed: st.caption("Ready to close")
        with fc3:
             save_btn = st.form_submit_button("💾 Save & Close", type="primary", use_container_width=True)

        if save_btn:
            update_data = {
                "brand": brand, "week_str": week_str, "owner_initials": owner, "sqe_name": sqe_name,
                "issue_info": info,
                "severity": severity, "supplier_sqa": supplier, "part_number": part_number,
                "quantity": quantity, "fkf_week": fkf,
                "repeat_issue": "Yes" if repeat == "Yes" else "No",
                "ftt_impact": "Yes" if ftt else "No",
                "field_impact": "Yes" if field else "No",
                "other_sites_impact": "Yes" if other else "No",
                "action_health": health,
                "d3_target": str(d3_tgt) if d3_tgt else "", "d3_status": d3_stat, "d3_remarks": d3_rem,
                "d5_target": str(d5_tgt) if d5_tgt else "", "d5_status": d5_stat, "d5_remarks": d5_rem,
                "d8_target": str(d8_tgt) if d8_tgt else "", "d8_status": d8_stat, "d8_remarks": d8_rem,
                "actions_completed": 1 if completed else 0
            }
            
            update_issue(conn, issue.id, update_data)
            
            if completed:
                close_issue(conn, issue.id)
            
            # Clear selection and RERUN to close dialog and refresh
            set_query_params(issue_id="")
            rerun()

# =========================
# EDITABLE METRICS TABLES
# =========================
def clear_selection_cb():
    # Callback to clear issue selection when table interaction happens
    set_query_params(issue_id="")

def render_metrics_tables(conn: sqlite3.Connection):
    st.markdown("## Operational Metrics")
    
    # 1. Recurrent Suppliers
    st.markdown('<div class="metric-header">RECURRENT SUPPLIERS (0KM & FIELD)</div>', unsafe_allow_html=True)
    df_recurrent = load_metric_df(conn, "recurrent", {
        "Supplier": ["", "", ""],
        "Occurrence": ["", "", ""], 
        "Action Plan": ["", "", ""]
    })

    # Fix for StreamlitAPIException: Ensure Occurrence is treated as string
    # Data editor expects text column but JSON load might infer int if data is simple numbers
    df_recurrent["Occurrence"] = df_recurrent["Occurrence"].astype(str)
    
    # Configure Occurrence as Text Column (NO VALIDATION to allow free text)
    column_config = {
        "Occurrence": st.column_config.TextColumn(
            "Occurrence",
            help="Number of occurrences"
            # Validation removed
        )
    }

    # Changed key to 'ed_rec_final' to reset state and fix type conflict (Int vs Str)
    edited_recurrent = st.data_editor(
        df_recurrent, 
        num_rows="dynamic", 
        key="ed_rec_final", 
        use_container_width=True,
        column_config=column_config
    )
    if not df_recurrent.equals(edited_recurrent):
        save_metric_df(conn, "recurrent", edited_recurrent)

    # 2. Supplier Development
    st.markdown('<div class="metric-header">SUPPLIER DEVELOPMENT</div>', unsafe_allow_html=True)
    df_dev = load_metric_df(conn, "sup_dev", {
        "Supplier": ["", ""],
        "Actions Identified": ["", ""],
        "Current Status": ["", ""]
    })
    
    edited_dev = st.data_editor(df_dev, num_rows="dynamic", key="ed_dev", use_container_width=True)
    if not df_dev.equals(edited_dev):
        save_metric_df(conn, "sup_dev", edited_dev)


# =========================
# MAIN APP LOOP
# =========================

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="🧩")
    inject_css()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, DB_FILE)
    conn = get_conn(db_path)
    init_db(conn)

    # 1. Header (Title Only - Logo Removed)
    st.markdown(f"<h1 style='margin-top: 5px;'>{APP_TITLE}</h1>", unsafe_allow_html=True)

    # 2. Sidebar Maintenance
    with st.sidebar:
        st.header("Maintenance")
        
        # Start Week Selector (Control Visible Weeks)
        current_wk_idx = max(0, current_iso_week() - 2) # Default to 2 weeks back
        try:
            default_start = ALL_WEEKS[current_wk_idx]
        except:
            default_start = ALL_WEEKS[0]
            
        start_week_val = st.selectbox("Start Week", ALL_WEEKS, index=ALL_WEEKS.index(default_start) if default_start in ALL_WEEKS else 0)
        
        # Calculate 8 weeks from start
        start_idx = ALL_WEEKS.index(start_week_val)
        # Slice 8 weeks, handling wrap around simply by stop
        visible_weeks = ALL_WEEKS[start_idx : start_idx + 8]
        if len(visible_weeks) < 8:
             visible_weeks += ALL_WEEKS[: 8 - len(visible_weeks)]

        st.markdown("---")
        
        # Fixed Reset DB Logic - Drop Tables instead of delete file
        if st.button("↻ Reset DB"):
            try:
                hard_reset_db(conn)
                st.success("Database reset. Refreshing...")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to reset DB: {e}")
        
        st.markdown("---")
        
        # Excel Export
        excel_data = export_to_excel(conn)
        if excel_data:
            st.download_button(
                label="📥 Export to Excel",
                data=excel_data,
                file_name=f"obeya_issues_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    # CHECK MODE (Board vs Detail)
    qp = get_query_params()
    sel_id = (qp.get("issue_id") or [""])[0]

    # DASHBOARD MODE (Always render board first)
    try:
        issues = fetch_issues(conn)
    except sqlite3.OperationalError as e:
        # Fallback if migration failed silently or schema is very broken
        hard_reset_db(conn)
        st.warning("Database schema reset due to critical error. Please refresh.")
        st.stop()

    # 4. Render Board
    render_board(issues, visible_weeks)

    # 5. Add Issue Section
    st.markdown("---")
    render_add_issue_section(conn, visible_weeks)

    # 6. Editable Metrics Tables
    st.markdown("---")
    render_metrics_tables(conn)

    # Legend
    st.markdown("---")
    l1, l2, l3, l4 = st.columns(4)
    with l1: st.caption("🟥 **Light Red**: FTT + High Sev")
    with l2: st.caption("🟧 **Light Orange**: FTT + Low Sev")
    with l3: st.caption("🟨 **Light Yellow**: Field Impact")
    with l4: st.caption("🔴/🟢 **Dot**: Action Health")

    # POPUP LOGIC: Trigger dialog if issue selected
    if sel_id:
        issue = fetch_issue_by_id(conn, sel_id)
        if issue:
            edit_issue_dialog(conn, issue)
        else:
            set_query_params(issue_id="")
            rerun()

if __name__ == "__main__":
    main()
