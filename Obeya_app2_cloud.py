# app.py — SQA India Digital Obeya Dashboard (CLOUD VERSION)
# Tech Stack: Python + Streamlit + Supabase (Cloud PostgreSQL)
# This version replaces local SQLite with persistent Supabase cloud storage

import os
import uuid
import pandas as pd
import io
import json
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional

import streamlit as st
from db_supabase import (
    get_supabase_client,
    create_issue,
    fetch_issues,
    fetch_issue_by_id,
    update_issue,
    close_issue,
    export_to_excel,
    load_metric_df,
    save_metric_df,
    hard_reset_db,
    Issue
)

# =========================
# CONFIG / CONSTANTS
# =========================

APP_TITLE = "SQ&D India Obeya Dashboard ☁️"
SCHEMA_VERSION = "3.0"

BRANDS_DEFAULT = ["QUESTER", "CRONER", "QUON"]
SEVERITY_OPTIONS = ["0P", "5P", "25P", "100P"]
ACTION_HEALTH_OPTIONS = ["On Track", "No Targets", "Delayed"]
YESNO = ["No", "Yes"]

ALL_WEEKS = [f"WK{i:02d}" for i in range(1, 54)]

# =========================
# UTILITIES (same as before)
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
    height: 180px;
    width: 170px;
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

/* Chip Styles */
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
    font-size: 1.05rem;
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

/* Custom Tooltip CSS */
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

/* Form Styles */
.stTextInput, .stSelectbox, .stDateInput, .stTextArea {
    margin-bottom: 0px !important;
}
div[data-testid="stForm"] {
    padding: 1rem;
    border-radius: 8px;
    border: 1px solid #e5e7eb;
}

/* Cloud Status Badge */
.cloud-badge {
    display: inline-block;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.85rem;
    font-weight: 600;
    margin-left: 10px;
}
</style>
        """,
        unsafe_allow_html=True,
    )

# =========================
# UI COMPONENTS
# =========================

def render_add_issue_section(visible_weeks: List[str]):
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
            col1, col2, col3, col4 = st.columns([1.5, 1, 1.5, 2.5])
            
            with col1:
                brand = st.selectbox("Brand", BRANDS_DEFAULT)
            with col2:
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
                    issue_id = create_issue({
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
                    if issue_id:
                        st.success("✅ Issue Published to Cloud!")
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

    # Construct headers
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

    table_content = "".join(rows_html)
    header_content = "".join(headers_html)
    
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
        display_name = issue.sqe_name if issue.sqe_name else issue.owner_initials
        tooltip_content = f"<b>SQE: {display_name}</b><br/>Supplier: {issue.supplier_sqa or 'NA'}<br/>{issue.issue_info}"
        safe_initials = html_escape(issue.owner_initials)

        chip = f"""<div class="tooltip-container"><a class="chip {chip_bg_class(issue)}" href="?issue_id={issue.id}" target="_self">{safe_initials}<span class="dot {dot_class(issue.action_health)}"></span><span class="tooltip-text">{tooltip_content}</span></a></div>"""
        chips_html.append(chip)
    
    inner_html = f'<div class="cell-grid {grid_class}">{"".join(chips_html)}</div>'
    return f'<td class="obeya-td {bg_class}">{inner_html}</td>'

# CHECK FOR DIALOG AVAILABILITY
if hasattr(st, "dialog"):
    dialog_decorator = st.dialog
elif hasattr(st, "experimental_dialog"):
    dialog_decorator = st.experimental_dialog
else:
    def dialog_decorator(title, width="medium"):
        def decorator(func):
            def wrapper(*args, **kwargs):
                st.markdown(f"### {title}")
                return func(*args, **kwargs)
            return wrapper
        return decorator

# COMPACT DIALOG
@dialog_decorator("Edit Issue", width="large")
def edit_issue_dialog(issue: Issue):
    with st.form("edit_issue_form"):
        c1, c2, c3, c4, c5 = st.columns([2, 1.5, 1.5, 1.5, 2])
        with c1: brand = st.selectbox("Brand", BRANDS_DEFAULT, index=BRANDS_DEFAULT.index(issue.brand))
        with c2: 
            wk_idx = ALL_WEEKS.index(issue.week_str) if issue.week_str in ALL_WEEKS else 0
            week_str = st.selectbox("Week", ALL_WEEKS, index=wk_idx)
        with c3: severity = st.selectbox("Severity", SEVERITY_OPTIONS, index=SEVERITY_OPTIONS.index(issue.severity) if issue.severity in SEVERITY_OPTIONS else 0)
        with c4: owner = st.text_input("Owner", value=issue.owner_initials)
        with c5: sqe_name = st.text_input("SQE Name", value=issue.sqe_name)

        info = st.text_area("Issue Info", value=issue.issue_info, height=68)

        r3c1, r3c2, r3c3, r3c4, r3c5 = st.columns(5)
        with r3c1: supplier = st.text_input("Supplier", value=issue.supplier_sqa)
        with r3c2: part_number = st.text_input("Part No.", value=issue.part_number)
        with r3c3: quantity = st.text_input("Qty", value=issue.quantity)
        with r3c4: fkf = st.text_input("FKF Wk", value=issue.fkf_week)
        with r3c5: repeat = st.selectbox("Repeat?", YESNO, index=YESNO.index(issue.repeat_issue))

        st.write("")
        r4c1, r4c2 = st.columns([3, 1])
        with r4c1:
            cc1, cc2, cc3 = st.columns(3)
            with cc1: ftt = st.checkbox("FTT Impact", value=(issue.ftt_impact=="Yes"))
            with cc2: field = st.checkbox("Field Impact", value=(issue.field_impact=="Yes"))
            with cc3: other = st.checkbox("Other Sites", value=(issue.other_sites_impact=="Yes"))
        with r4c2:
            health = st.selectbox("Action Health", ACTION_HEALTH_OPTIONS, index=ACTION_HEALTH_OPTIONS.index(issue.action_health) if issue.action_health in ACTION_HEALTH_OPTIONS else 1)

        st.markdown("---")
        ac1, ac2, ac3 = st.columns(3)
        
        with ac1:
            st.caption("**D3 (Containment)**")
            d3_tgt = st.date_input("D3 Tgt", value=parse_date_str(issue.d3_target), label_visibility="collapsed")
            d3_stat = st.text_input("D3 Stat", value=issue.d3_status, placeholder="Status", label_visibility="collapsed")
            d3_rem = st.text_area("D3 Rem", value=issue.d3_remarks, height=68, placeholder="Remarks", label_visibility="collapsed")

        with ac2:
            st.caption("**D5 (Root Cause)**")
            d5_tgt = st.date_input("D5 Tgt", value=parse_date_str(issue.d5_target), label_visibility="collapsed")
            d5_stat = st.text_input("D5 Stat", value=issue.d5_status, placeholder="Status", label_visibility="collapsed")
            d5_rem = st.text_area("D5 Rem", value=issue.d5_remarks, height=68, placeholder="Remarks", label_visibility="collapsed")
        
        with ac3:
            st.caption("**D8 (Closure)**")
            d8_tgt = st.date_input("D8 Tgt", value=parse_date_str(issue.d8_target), label_visibility="collapsed")
            d8_stat = st.text_input("D8 Stat", value=issue.d8_status, placeholder="Status", label_visibility="collapsed")
            d8_rem = st.text_area("D8 Rem", value=issue.d8_remarks, height=68, placeholder="Remarks", label_visibility="collapsed")

        st.markdown("---")
        
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
            
            update_issue(issue.id, update_data)
            
            if completed:
                close_issue(issue.id)
            
            set_query_params(issue_id="")
            rerun()

# =========================
# EDITABLE METRICS TABLES
# =========================

def render_metrics_tables():
    st.markdown("## Operational Metrics")
    
    st.markdown('<div class="metric-header">RECURRENT SUPPLIERS (0KM & FIELD)</div>', unsafe_allow_html=True)
    df_recurrent = load_metric_df("recurrent", {
        "Supplier": ["", "", ""],
        "Occurrence": ["", "", ""], 
        "Action Plan": ["", "", ""]
    })

    df_recurrent["Occurrence"] = df_recurrent["Occurrence"].astype(str)
    
    column_config = {
        "Occurrence": st.column_config.TextColumn("Occurrence", help="Number of occurrences")
    }

    edited_recurrent = st.data_editor(
        df_recurrent, 
        num_rows="dynamic", 
        key="ed_rec_final", 
        use_container_width=True,
        column_config=column_config
    )
    if not df_recurrent.equals(edited_recurrent):
        save_metric_df("recurrent", edited_recurrent)

    st.markdown('<div class="metric-header">SUPPLIER DEVELOPMENT</div>', unsafe_allow_html=True)
    df_dev = load_metric_df("sup_dev", {
        "Supplier": ["", ""],
        "Actions Identified": ["", ""],
        "Current Status": ["", ""]
    })
    
    edited_dev = st.data_editor(df_dev, num_rows="dynamic", key="ed_dev", use_container_width=True)
    if not df_dev.equals(edited_dev):
        save_metric_df("sup_dev", edited_dev)


# =========================
# MAIN APP LOOP
# =========================

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="☁️")
    inject_css()

    # Verify Supabase connection
    try:
        _ = get_supabase_client()
    except Exception as e:
        st.error(f"❌ Cannot connect to Supabase: {str(e)}")
        st.info("📖 See SETUP_SUPABASE.md for setup instructions")
        st.stop()

    # 1. Header
    col1, col2 = st.columns([1, 0.3])
    with col1:
        st.markdown(f"<h1 style='margin-top: 5px;'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    with col2:
        st.markdown('<span class="cloud-badge">☁️ CLOUD LIVE</span>', unsafe_allow_html=True)

    # 2. Sidebar Maintenance
    with st.sidebar:
        st.header("⚙️ Maintenance")
        
        # Start Week Selector
        current_wk_idx = max(0, current_iso_week() - 2)
        try:
            default_start = ALL_WEEKS[current_wk_idx]
        except:
            default_start = ALL_WEEKS[0]
            
        start_week_val = st.selectbox("Start Week", ALL_WEEKS, index=ALL_WEEKS.index(default_start) if default_start in ALL_WEEKS else 0)
        
        # Calculate 8 weeks from start
        start_idx = ALL_WEEKS.index(start_week_val)
        visible_weeks = ALL_WEEKS[start_idx : start_idx + 8]
        if len(visible_weeks) < 8:
             visible_weeks += ALL_WEEKS[: 8 - len(visible_weeks)]

        st.markdown("---")
        
        # Reset DB
        if st.button("↻ Reset Cloud DB"):
            if st.checkbox("⚠️ I understand this will delete ALL data"):
                try:
                    hard_reset_db()
                    st.success("Database reset!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")
        
        st.markdown("---")
        
        # Excel Export
        excel_data = export_to_excel()
        if excel_data:
            st.download_button(
                label="📥 Export to Excel",
                data=excel_data,
                file_name=f"obeya_issues_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        st.markdown("---")
        st.caption("v3.0 - Cloud Edition")
        st.caption("🟢 Data saved to cloud")

    # CHECK MODE
    qp = get_query_params()
    sel_id = (qp.get("issue_id") or [""])[0]

    # FETCH ISSUES
    try:
        issues = fetch_issues()
    except Exception as e:
        st.error(f"❌ Error fetching issues: {str(e)}")
        st.stop()

    # 4. Render Board
    render_board(issues, visible_weeks)

    # 5. Add Issue Section
    st.markdown("---")
    render_add_issue_section(visible_weeks)

    # 6. Editable Metrics Tables
    st.markdown("---")
    render_metrics_tables()

    # Legend
    st.markdown("---")
    l1, l2, l3, l4 = st.columns(4)
    with l1: st.caption("🟥 **Red**: FTT + High Sev")
    with l2: st.caption("🟧 **Orange**: FTT + Low Sev")
    with l3: st.caption("🟨 **Yellow**: Field Impact")
    with l4: st.caption("🔴/🟢 **Dot**: Action Health")

    # POPUP LOGIC
    if sel_id:
        issue = fetch_issue_by_id(sel_id)
        if issue:
            edit_issue_dialog(issue)
        else:
            set_query_params(issue_id="")
            rerun()

if __name__ == "__main__":
    main()
