
import io
import streamlit as st
import pandas as pd

from business_finder_core import (
    FALLBACK_CATEGORIES,
    MAIN_CATEGORY_MAP,
    US_STATES,
    load_cities_by_state_from_csv_obj,
    build_subcategory_plan,
    run_search_plan,
)

st.set_page_config(page_title="Automatic Business Finder", layout="wide")
st.title("Automatic Business Finder")
st.caption("BBB search app converted from desktop Tkinter to a shareable web app.")

with st.sidebar:
    st.header("1) Upload cities.csv")
    st.write("CSV must include: city, state_id, state_name")
    cities_file = st.file_uploader("Upload cities.csv", type=["csv"])

    st.header("2) Search options")
    state = st.selectbox("State", US_STATES, index=US_STATES.index("IL") if "IL" in US_STATES else 0)

    cities_by_state = load_cities_by_state_from_csv_obj(cities_file) if cities_file else {}
    city_options = cities_by_state.get(state, [])
    selected_cities = st.multiselect("Cities", city_options)

    selected_mains = st.multiselect(
        "Main categories",
        BBB_POPULAR_MAIN_CATEGORIES,
        default=[BBB_POPULAR_MAIN_CATEGORIES[0]] if BBB_POPULAR_MAIN_CATEGORIES else [],
    )

    use_all_subcategories = st.checkbox("Use all subcategories from selected main categories", value=True)

    available_subs = []
    for main in selected_mains:
        available_subs.extend(MAIN_CATEGORY_MAP.get(main, [main]))
    available_subs = sorted(set(available_subs), key=str.lower)

    selected_subs = []
    if not use_all_subcategories:
        selected_subs = st.multiselect("Subcategories", available_subs)

    enrich_emails = st.checkbox("Try to find business emails from websites", value=True)
    output_name = st.text_input("Output Excel name", value="business_results.xlsx")

run_clicked = st.button("Run search", type="primary", use_container_width=True)

log_box = st.empty()
status_box = st.empty()

if run_clicked:
    if cities_file is None:
        st.error("Upload cities.csv first.")
    elif not selected_mains:
        st.error("Select at least one main category.")
    elif not selected_cities:
        st.error("Select at least one city.")
    else:
        search_plan = build_subcategory_plan(
            selected_mains=selected_mains,
            selected_subs=selected_subs,
            use_all_subcategories=use_all_subcategories,
        )
        if not search_plan:
            st.error("No subcategories were selected.")
        else:
            logs = []
            def logger(msg):
                logs.append(str(msg))
                log_box.text("\n".join(logs[-200:]))

            status_box.info("Running search...")
            try:
                output_path, saved = run_search_plan(
                    search_plan=search_plan,
                    cities=selected_cities,
                    state=state,
                    output_path=output_name,
                    enrich_emails=enrich_emails,
                    logger=logger,
                )
                status_box.success(f"Done. Saved {saved} row(s).")
                with open(output_path, "rb") as f:
                    data = f.read()
                st.download_button(
                    "Download Excel file",
                    data=data,
                    file_name=output_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

                try:
                    df = pd.read_excel(io.BytesIO(data))
                    st.subheader("Preview")
                    st.dataframe(df.head(50), use_container_width=True)
                except Exception:
                    pass
            except Exception as e:
                status_box.error(f"Error: {e}")
