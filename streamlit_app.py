
import io
import os
import threading
from pathlib import Path

import pandas as pd
import streamlit as st

from business_finder_core import (
    US_STATES,
    get_all_selectable_categories,
    load_cities_by_state_from_csv_obj,
    initialize_job,
    run_job_with_resume,
    read_job_state,
    make_safe_job_id,
)

st.set_page_config(page_title="Automatic Business Finder", layout="wide")
st.title("Automatic Business Finder")
st.caption("Stable version with background jobs, CSV backup, checkpoint resume, and safe mode for large cities.")

JOBS_DIR = Path("job_runs")
JOBS_DIR.mkdir(exist_ok=True)

if "active_logs" not in st.session_state:
    st.session_state.active_logs = {}
if "active_threads" not in st.session_state:
    st.session_state.active_threads = {}

def append_log(job_id: str, message: str):
    st.session_state.active_logs.setdefault(job_id, [])
    st.session_state.active_logs[job_id].append(str(message))
    st.session_state.active_logs[job_id] = st.session_state.active_logs[job_id][-500:]

def start_background_job(job_id: str, job_dir: str, mode: str, enrich_emails: bool):
    def logger(msg):
        append_log(job_id, msg)
    def runner():
        try:
            run_job_with_resume(job_dir=job_dir, mode=mode, enrich_emails=enrich_emails, logger=logger)
        except Exception as e:
            append_log(job_id, f"ERROR: {e}")
    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    st.session_state.active_threads[job_id] = thread

with st.sidebar:
    st.header("1) Upload cities.csv")
    st.write("CSV must include: city, state_id, state_name")
    cities_file = st.file_uploader("Upload cities.csv", type=["csv"])
    st.header("2) Search options")
    state = st.selectbox("State", US_STATES, index=US_STATES.index("IL") if "IL" in US_STATES else 0)
    cities_by_state = load_cities_by_state_from_csv_obj(cities_file) if cities_file else {}
    city_options = cities_by_state.get(state, [])
    selected_cities = st.multiselect("Cities", city_options)
    all_categories = get_all_selectable_categories()
    selected_categories = st.multiselect("Select any BBB categories", all_categories)
    mode = st.radio("Run mode", ["safe", "fast"], index=0, help="Safe mode uses fewer workers and saves more often.")
    enrich_emails = st.checkbox("Email enrichment", value=False, help="Off by default for stability on large jobs.")
    output_name = st.text_input("Output Excel name", value="business_results.xlsx")
    st.header("3) Resume existing job")
    existing_jobs = sorted([p.name for p in JOBS_DIR.iterdir() if p.is_dir()])
    selected_job_to_resume = st.selectbox("Choose job folder", [""] + existing_jobs)

c1, c2, c3 = st.columns(3)
start_clicked = c1.button("Start new job", type="primary", use_container_width=True)
resume_clicked = c2.button("Resume selected job", use_container_width=True)
c3.button("Refresh status", use_container_width=True)

if start_clicked:
    if cities_file is None:
        st.error("Upload cities.csv first.")
    elif not selected_cities:
        st.error("Select at least one city.")
    elif not selected_categories:
        st.error("Select at least one category.")
    else:
        job_id = make_safe_job_id(f"{state}_{'_'.join(selected_cities[:2])}_{len(selected_categories)}cats")
        job_dir = str(JOBS_DIR / job_id)
        initialize_job(job_dir, selected_categories, selected_cities, state, output_name)
        st.session_state.active_logs[job_id] = []
        append_log(job_id, f"Job created: {job_id}")
        append_log(job_id, f"Mode: {mode}")
        append_log(job_id, f"Email enrichment: {enrich_emails}")
        start_background_job(job_id, job_dir, mode, enrich_emails)
        st.success(f"Started background job: {job_id}")

if resume_clicked:
    if not selected_job_to_resume:
        st.error("Select a job to resume.")
    else:
        job_id = selected_job_to_resume
        job_dir = str(JOBS_DIR / job_id)
        state_data = read_job_state(job_dir)
        if not state_data:
            st.error("Could not read that job.")
        else:
            st.session_state.active_logs.setdefault(job_id, [])
            append_log(job_id, f"Resuming job: {job_id}")
            start_background_job(job_id, job_dir, mode, enrich_emails)
            st.success(f"Resumed background job: {job_id}")

st.subheader("Jobs")
job_dirs = sorted([p for p in JOBS_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)

if not job_dirs:
    st.info("No jobs yet.")
else:
    for job_path in job_dirs[:20]:
        job_state = read_job_state(str(job_path))
        if not job_state:
            continue
        with st.expander(f"{job_path.name} — {job_state.get('status', 'unknown')}"):
            m1, m2, m3 = st.columns(3)
            m1.metric("Saved rows", job_state.get("saved_rows", 0))
            m2.metric("Cities", len(job_state.get("cities", [])))
            m3.metric("Categories", len(job_state.get("selected_categories", [])))
            st.write("State:", job_state.get("state", ""))
            st.write("Current city index:", job_state.get("current_city_index", 0))
            st.write("Current main index:", job_state.get("current_main_index", 0))
            st.write("Current sub index:", job_state.get("current_sub_index", 0))
            if job_state.get("last_error"):
                st.error(job_state["last_error"])
            log_text = "\n".join(st.session_state.active_logs.get(job_path.name, []))
            st.text_area("Live logs", value=log_text, height=200, key=f"logs_{job_path.name}")
            csv_path = job_state.get("csv_path", "")
            excel_path = job_state.get("excel_path", "")
            if csv_path and os.path.exists(csv_path):
                with open(csv_path, "rb") as f:
                    csv_data = f.read()
                st.download_button("Download CSV backup", data=csv_data, file_name=os.path.basename(csv_path), mime="text/csv", key=f"csv_{job_path.name}")
                try:
                    df = pd.read_csv(io.BytesIO(csv_data))
                    st.dataframe(df.head(50), use_container_width=True)
                except Exception:
                    pass
            if excel_path and os.path.exists(excel_path):
                with open(excel_path, "rb") as f:
                    excel_data = f.read()
                st.download_button("Download Excel file", data=excel_data, file_name=os.path.basename(excel_path), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"xlsx_{job_path.name}")

st.info("Safe mode uses fewer workers, saves every 5 rows to CSV, and is recommended for large cities. Resume continues from the last completed city/category/subcategory.")
