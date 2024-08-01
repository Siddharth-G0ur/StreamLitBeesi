import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

st.set_page_config(page_title="Android App Overview", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

if st.button("← Back to Home"):
    st.switch_page("home.py")

st.markdown("""
<style>
    .stDataFrame {
        width: 100%;
    }
    .dataframe {
        font-size: 12px;
    }
    .dataframe th {
        background-color: #f0f2f6;
        color: #31333F;
        font-weight: bold;
        text-align: left !important;
    }
    .dataframe td {
        text-align: right !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("Android App Overview")

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def run_query(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

sql_query = """
WITH
 daily_user_activity AS (
 SELECT
 event_date,
 user_pseudo_id,
 MAX(CASE
 WHEN event_name = 'first_open' THEN 1
 ELSE 0
 END) AS had_first_open,
 MAX(CASE
 WHEN event_name = 'first_open' AND param.key = 'previous_first_open_count' THEN param.value.int_value
 ELSE NULL
 END) AS previous_first_open_count,
 MAX(CASE
 WHEN event_name IN('screen_load', 'view_click') THEN 1
 ELSE 0
 END) AS custom_event
 FROM
 `swap-vc-prod.analytics_325691371.events_*`,
 UNNEST(event_params) AS param
 WHERE
 platform = 'ANDROID'
 GROUP BY
 event_date,
 user_pseudo_id
 ),
 user_classification AS (
 SELECT
 event_date,
 user_pseudo_id,
 CASE
 WHEN had_first_open = 1 AND previous_first_open_count = 0 THEN 'fresh_install'
 WHEN had_first_open = 1 AND previous_first_open_count > 0 THEN 'reinstall'
 WHEN had_first_open = 1 THEN 'new_user'
 ELSE 'returning_user'
 END AS user_type,
 custom_event
 FROM
 daily_user_activity
 )
SELECT
 event_date,
 COUNT(DISTINCT user_pseudo_id) AS total_users,
 COUNT(DISTINCT CASE WHEN user_type IN ('fresh_install', 'reinstall', 'new_user') THEN user_pseudo_id END) AS new_users,
 COUNT(DISTINCT CASE WHEN user_type = 'returning_user' THEN user_pseudo_id END) AS returning_users,
 COUNT(DISTINCT CASE WHEN user_type = 'fresh_install' THEN user_pseudo_id END) AS fresh_installs,
 COUNT(DISTINCT CASE WHEN user_type = 'reinstall' THEN user_pseudo_id END) AS reinstalls,
 COUNT(DISTINCT CASE WHEN custom_event = 1 THEN user_pseudo_id END) AS users_with_custom_event
FROM
 user_classification
GROUP BY
 event_date
ORDER BY
 event_date DESC
"""

df = run_query(sql_query)

# Convert event_date to datetime and then to string in 'YYYY-MM-DD' format
df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

# Set event_date as index
df.set_index('event_date', inplace=True)

# Date Filter
with st.expander("Date Filter", expanded=True):
    min_date = datetime.strptime(df.index.min(), '%Y-%m-%d').date()
    max_date = datetime.strptime(df.index.max(), '%Y-%m-%d').date()
    date_range = st.date_input('Select Date Range', [min_date, max_date])

# Apply date filter
if len(date_range) == 2:
    start_date, end_date = [d.strftime('%Y-%m-%d') for d in date_range]
    df = df[(df.index >= start_date) & (df.index <= end_date)]

# Sort the dataframe
df = df.sort_index(ascending=False)

# Display the dataframe
st.dataframe(
    df.style
        .format({col: '{:,.0f}' for col in df.columns})
        .set_properties(**{'text-align': 'right'})
        .set_table_styles([
            {'selector': 'th', 'props': [('text-align', 'left')]},
            {'selector': 'td', 'props': [('text-align', 'right')]},
        ]),
    use_container_width=True,
    height=400
)

# Prepare CSV data
csv = df.to_csv()
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="user_activity_data.csv",
    mime="text/csv",
)