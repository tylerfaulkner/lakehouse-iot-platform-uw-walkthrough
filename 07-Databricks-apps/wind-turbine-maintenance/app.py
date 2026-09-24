"""
Wind Turbine Maintenance Queue
Databricks App — Streamlit frontend for operators to track and update turbine repair status.

The upstream ML pipeline has already run predictions and written flagged turbines
to a Delta table in Unity Catalog. This app reads that table and lets operators
change statuses and leave notes — writing back to the same Delta table.
"""

import os
import streamlit as st
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from datetime import datetime, date
import pandas as pd

# ---------------------------------------------------------------------------
# 1. SPARK SESSION
# ---------------------------------------------------------------------------
# databricks.connect picks up credentials automatically from the Databricks
# Apps runtime environment — no tokens or cluster IDs needed here.

@st.cache_resource(show_spinner="Connecting to Databricks...")
def get_spark():
    """Return a cached Spark session backed by Databricks Connect."""
    return DatabricksSession.builder.serverless(True).getOrCreate()

spark = get_spark()

# ---------------------------------------------------------------------------
# 2. CONSTANTS
# ---------------------------------------------------------------------------

CATALOG = os.getenv("IOT_CATALOG", "workspace")
SCHEMA  = os.getenv("IOT_SCHEMA", "dbdemos_iot_platform")
TABLE   = "maintenance_queue"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

STATUS_OPTIONS = ["Pending", "In Progress", "Resolved"]

# Severity thresholds used for color-coding throughout the app
SEV_HIGH   = 0.7
SEV_MEDIUM = 0.4

# ---------------------------------------------------------------------------
# 3. DATA HELPERS
# ---------------------------------------------------------------------------

def load_data() -> pd.DataFrame:
    """
    Read the full maintenance_queue Delta table and return a pandas DataFrame.
    Casting to pandas here because Streamlit works natively with pandas.
    """
    df = (
        spark.table(FULL_TABLE)
        .orderBy(F.col("severity_score").desc())   # highest severity first
    )
    return df.toPandas()


def write_update(turbine_id: str, new_status: str, notes: str) -> None:
    """
    Persist a status + notes update back to the Delta table.

    We use a MERGE statement (upsert) so we only touch the one row that
    changed rather than overwriting the whole table.
    """
    # Escape single quotes in user-supplied notes to prevent SQL injection
    safe_notes  = notes.replace("'", "''")
    safe_status = new_status.replace("'", "''")
    safe_id     = turbine_id.replace("'", "''")
    now         = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(f"""
        MERGE INTO {FULL_TABLE} AS target
        USING (
            SELECT
                '{safe_id}'     AS turbine_id,
                '{safe_status}' AS status,
                '{safe_notes}'  AS technician_notes,
                TIMESTAMP '{now}' AS last_updated
        ) AS source
        ON target.turbine_id = source.turbine_id
        WHEN MATCHED THEN UPDATE SET
            target.status            = source.status,
            target.technician_notes  = source.technician_notes,
            target.last_updated      = source.last_updated
    """)


def severity_label(score: float) -> str:
    """Map a numeric severity score to a human-readable level."""
    if score > SEV_HIGH:
        return "🔴 High"
    elif score >= SEV_MEDIUM:
        return "🟡 Medium"
    else:
        return "🟢 Low"


# ---------------------------------------------------------------------------
# 4. SESSION STATE — track the last refresh time across reruns
# ---------------------------------------------------------------------------

if "last_refreshed" not in st.session_state:
    st.session_state.last_refreshed = datetime.now()

if "data" not in st.session_state:
    st.session_state.data = load_data()

# ---------------------------------------------------------------------------
# 5. SIDEBAR
# ---------------------------------------------------------------------------

with st.sidebar:
    # Logo placeholder — swap the emoji for an st.image() call once you have an asset
    st.markdown("# 🌬️")
    st.title("Turbine Maintenance Queue")
    st.divider()

    st.caption(f"Last refreshed: {st.session_state.last_refreshed.strftime('%H:%M:%S')}")

    if st.button("🔄 Refresh Data", use_container_width=True):
        # Clear cached data and reload from Delta
        st.session_state.data = load_data()
        st.session_state.last_refreshed = datetime.now()
        st.rerun()

    st.divider()
    st.caption("Filter the queue below to narrow your view.")

# ---------------------------------------------------------------------------
# 6. LOAD DATA INTO A LOCAL VARIABLE FOR THIS RUN
# ---------------------------------------------------------------------------

df: pd.DataFrame = st.session_state.data

# ---------------------------------------------------------------------------
# 7. SUMMARY HEADER — metric cards
# ---------------------------------------------------------------------------

st.header("🌬️ Turbine Maintenance Queue")
st.caption("Real-time view of turbines flagged by the predictive maintenance pipeline.")

today = date.today()

# Narrow to records flagged today for the "today" counters
df["flagged_date"] = pd.to_datetime(df["flagged_at"]).dt.date
today_df = df[df["flagged_date"] == today]

col1, col2, col3 = st.columns(3)

with col1:
    pending_count = int((today_df["status"] == "Pending").sum())
    st.metric("⏳ Pending Today", pending_count)

with col2:
    in_progress_count = int((today_df["status"] == "In Progress").sum())
    st.metric("🔧 In Progress Today", in_progress_count)

with col3:
    resolved_count = int((today_df["status"] == "Resolved").sum())
    st.metric("✅ Resolved Today", resolved_count)

st.divider()

# ---------------------------------------------------------------------------
# 8. FILTERS
# ---------------------------------------------------------------------------

st.subheader("Maintenance Queue")

filter_col1, filter_col2 = st.columns([1, 2])

with filter_col1:
    status_filter = st.multiselect(
        "Filter by status",
        options=STATUS_OPTIONS,
        default=STATUS_OPTIONS,   # show all by default
    )

with filter_col2:
    search_term = st.text_input("Search by turbine ID or location", placeholder="e.g. T-042 or Austin")

# Apply filters to the DataFrame
filtered_df = df[df["status"].isin(status_filter)].copy()

if search_term:
    mask = (
        filtered_df["turbine_id"].str.contains(search_term, case=False, na=False)
        | filtered_df["location"].str.contains(search_term, case=False, na=False)
    )
    filtered_df = filtered_df[mask]

# ---------------------------------------------------------------------------
# 9. SEVERITY COLOR-CODING
# ---------------------------------------------------------------------------
# Streamlit's st.dataframe supports column styling via pandas Styler.

def color_severity(val: float) -> str:
    """Return a CSS background color string based on severity score."""
    if val > SEV_HIGH:
        return "background-color: #FFCCCC"   # light red
    elif val >= SEV_MEDIUM:
        return "background-color: #FFF3CC"   # light yellow
    else:
        return "background-color: #CCFFCC"   # light green


# Build a display copy with a friendly Severity Level column
display_df = filtered_df[[
    "turbine_id", "location", "predicted_failure_type",
    "severity_score", "status", "flagged_at", "technician_notes", "last_updated"
]].copy()

display_df.insert(4, "severity_level", display_df["severity_score"].apply(severity_label))

styled = (
    display_df.style
    .applymap(color_severity, subset=["severity_score"])
    .format({"severity_score": "{:.2f}"})
)

st.dataframe(
    styled,
    use_container_width=True,
    hide_index=True,
    column_config={
        "turbine_id":             st.column_config.TextColumn("Turbine ID"),
        "location":               st.column_config.TextColumn("Location"),
        "predicted_failure_type": st.column_config.TextColumn("Failure Type"),
        "severity_score":         st.column_config.NumberColumn("Severity", format="%.2f"),
        "severity_level":         st.column_config.TextColumn("Level"),
        "status":                 st.column_config.TextColumn("Status"),
        "flagged_at":             st.column_config.DatetimeColumn("Flagged At", format="MMM D, HH:mm"),
        "technician_notes":       st.column_config.TextColumn("Notes"),
        "last_updated":           st.column_config.DatetimeColumn("Last Updated", format="MMM D, HH:mm"),
    },
)

st.caption(f"Showing {len(filtered_df)} of {len(df)} total turbines.")

# ---------------------------------------------------------------------------
# 10. UPDATE PANEL
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Update a Turbine Record")

# Let the operator pick which turbine they want to act on
turbine_options = filtered_df["turbine_id"].tolist()

if not turbine_options:
    st.info("No turbines match the current filter. Adjust the filters above to see records.")
else:
    selected_id = st.selectbox(
        "Select turbine to update",
        options=turbine_options,
        help="The list reflects your current filter selection above.",
    )

    # Pull the current row for this turbine
    current_row = df[df["turbine_id"] == selected_id].iloc[0]

    with st.expander(f"✏️ Editing — {selected_id} ({current_row['location']})", expanded=True):

        # Show a quick summary so the operator has context without scrolling up
        info_col1, info_col2, info_col3 = st.columns(3)
        with info_col1:
            st.metric("Failure Type", current_row["predicted_failure_type"])
        with info_col2:
            st.metric("Severity Score", f"{current_row['severity_score']:.2f}")
        with info_col3:
            st.metric("Current Status", current_row["status"])

        st.markdown("---")

        # Status dropdown — pre-set to the turbine's current status
        new_status = st.selectbox(
            "New Status",
            options=STATUS_OPTIONS,
            index=STATUS_OPTIONS.index(current_row["status"]),
        )

        # Notes text area — pre-fill with any existing notes
        existing_notes = current_row["technician_notes"] if pd.notna(current_row["technician_notes"]) else ""
        new_notes = st.text_area(
            "Technician Notes",
            value=existing_notes,
            height=120,
            placeholder="Describe the issue, parts needed, or resolution steps...",
        )

        # Submit button — writes to Delta and refreshes the in-memory data
        if st.button("💾 Submit Update", type="primary", use_container_width=True):
            with st.spinner("Writing update to Delta table..."):
                write_update(selected_id, new_status, new_notes)
                # Reload data so the table above reflects the change immediately
                st.session_state.data = load_data()
                st.session_state.last_refreshed = datetime.now()

            st.success(f"Updated **{selected_id}** → {new_status}")
            st.rerun()   # refresh the whole page so the queue table reflects the edit
