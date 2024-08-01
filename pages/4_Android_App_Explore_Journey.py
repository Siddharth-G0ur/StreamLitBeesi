import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="Android App Explore Journey Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

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

st.title("Android App Explore Journey Dashboard")

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def run_query(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

sql_query = """
SELECT
    event_date,
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE KEY = 'beesi_user_id') AS beesi_user_id,
    CASE
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'home_screen' THEN 'User landed on homepage'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'home_screen' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'view_id') = 'kyun_karni_hai_beesi' THEN 'User click on kyun karni hai beesi'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'kyun_karni_hai_beesi' THEN 'User land on kyun karni hai beesi'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'kyun_karni_hai_beesi' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'view_id') = 'cool' THEN 'User click on cool on kyun karni hai beesi'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'kyun_karni_hai_beesi' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'view_id') = 'back_button' THEN 'User click on back button on kyun karni hai beesi'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'home_screen' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'view_id') = 'beesi_kya_hai' THEN 'User click on beesi kya hai'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'beesi_kya_hai' THEN 'User land on beesi kya hai screen'
      WHEN event_name = 'play_player' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'beesi_kya_hai' THEN 'User click on play button on beesi kya hai screen'
      WHEN event_name = 'pause_player' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'beesi_kya_hai' THEN 'User click on pause button on sampling UI video'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'beesi_kya_hai' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'view_id') = 'back_button' THEN 'User click on back button on beesi kya hai screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'beesi_kya_hai' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'view_id') = 'create_beesi_group' THEN 'User click on create beesi group on beesi kya hai screen'
    END AS actions,
    COALESCE(
      CASE
        WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'duration') IS NOT NULL THEN
          CAST(SPLIT((SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'duration'), ':')[OFFSET(0)] AS INT64) * 60 +
          CAST(SPLIT((SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'duration'), ':')[OFFSET(1)] AS INT64)
        ELSE NULL
      END,
      0
    ) AS duration_seconds,
    geo.country as country,
    geo.region as region,
    geo.city as city,
    app_info.version as app_version,
    device.operating_system_version as os_version
  FROM
    `swap-vc-prod.analytics_325691371.events_*`
    where platform = 'ANDROID'
"""

df = run_query(sql_query)
df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d')

# Function to clean options
def clean_options(options):
    return sorted([opt for opt in options if opt is not None and opt != ''])

# Create columns for filter categories
col1, col2 = st.columns(2)

# Date Filter
with col1:
    with st.expander("Date Filter", expanded=True):
        start_date = st.date_input("Start Date", value=df['event_date'].min(), key='event_start_date')
        end_date = st.date_input("End Date", value=df['event_date'].max(), key='event_end_date')

# Other Filters
with col2:
    with st.expander("Version Filters", expanded=True):
        os_version_options = clean_options(df['os_version'].unique())
        os_version_filter = st.multiselect('OS Version', options=os_version_options, key='os_version_filter')
        app_version_options = clean_options(df['app_version'].unique())
        app_version_filter = st.multiselect('App Version', options=app_version_options, key='app_version_filter')

# Location Filter
with st.expander("Location Filter", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        country_options = clean_options(df['country'].unique())
        country_filter = st.multiselect('Country', options=country_options, key='country_filter')
    with col2:
        region_options = clean_options(df['region'].unique())
        region_filter = st.multiselect('Region', options=region_options, key='region_filter')
    with col3:
        city_options = clean_options(df['city'].unique())
        city_filter = st.multiselect('City', options=city_options, key='city_filter')

# Apply filters
df = df[(df['event_date'].dt.date >= start_date) & (df['event_date'].dt.date <= end_date)]

if country_filter:
    df = df[df['country'].isin(country_filter)]
if region_filter:
    df = df[df['region'].isin(region_filter)]
if city_filter:
    df = df[df['city'].isin(city_filter)]
if os_version_filter:
    df = df[df['os_version'].isin(os_version_filter)]
if app_version_filter:
    df = df[df['app_version'].isin(app_version_filter)]

# Calculate Total Users and Logged-in Users
total_users = df.groupby('event_date')['user_pseudo_id'].nunique().reset_index()
logged_in_users = df.groupby('event_date')['beesi_user_id'].nunique().reset_index()

# Merge these counts
user_counts = total_users.merge(logged_in_users, on='event_date', suffixes=('_total', '_logged_in'))

# Create pivot table for actions
pivot_df = df.pivot_table(
    values='beesi_user_id',
    index='event_date',
    columns='actions',
    aggfunc='nunique',
    fill_value=0
)

# Reset index to make 'event_date' a column
pivot_df = pivot_df.reset_index()

# Merge the user counts with the pivot table
pivot_df = pivot_df.merge(user_counts, on='event_date', how='outer')

# Rename the columns
pivot_df = pivot_df.rename(columns={'user_pseudo_id': 'Total Users', 'beesi_user_id': 'Logged-in Users'})

def calculate_watch_duration(group):
    # Get the maximum duration for each user
    max_durations = group.groupby('beesi_user_id')['duration_seconds'].max()
    total_users = len(max_durations)
    
    if total_users == 0:
        return pd.Series({
            'User didnt watch the video': 0,
            'User watched the video for 1-10 seconds': 0,
            'User watched the video for 11-30 seconds': 0,
            'User watched the video for 31-60 seconds': 0,
            'User watched the video for 61-120 seconds': 0,
            'User watched the video for more than 120 seconds': 0,
        })
    
    return pd.Series({
        'User didnt watch the video': (max_durations == 0).sum() / total_users * 100,
        'User watched the video for 1-10 seconds': ((max_durations > 0) & (max_durations <= 10)).sum() / total_users * 100,
        'User watched the video for 11-30 seconds': ((max_durations > 10) & (max_durations <= 30)).sum() / total_users * 100,
        'User watched the video for 31-60 seconds': ((max_durations > 30) & (max_durations <= 60)).sum() / total_users * 100,
        'User watched the video for 61-120 seconds': ((max_durations > 60) & (max_durations <= 120)).sum() / total_users * 100,
        'User watched the video for more than 120 seconds': (max_durations > 120).sum() / total_users * 100,
    })

# Update this line to use groupby().apply() with grouped=False
watch_durations = df.groupby('event_date', group_keys=False).apply(calculate_watch_duration).reset_index()

watch_durations = df.groupby('event_date').apply(calculate_watch_duration).reset_index()
watch_durations.columns = ['event_date'] + list(watch_durations.columns[1:])

# Merge watch durations with pivot_df
pivot_df = pivot_df.merge(watch_durations, on='event_date', how='left')

# Fill NaN values with 0
pivot_df = pivot_df.fillna(0)

# We don't need to calculate percentages for watch duration columns anymore
# as they are already in percentage form

# For other columns, calculate percentages avoiding division by zero
for col in pivot_df.columns:
    if col not in ['event_date', 'Total Users', 'Logged-in Users'] and col not in watch_durations.columns:
        pivot_df[col] = pivot_df.apply(lambda row: 0 if row['Logged-in Users'] == 0 else row[col] / row['Logged-in Users'] * 100, axis=1)

# Define the new column order
column_order = [
    'event_date',
    'Total Users',
    'Logged-in Users',
    'User landed on homepage',
    'User click on kyun karni hai beesi',
    'User land on kyun karni hai beesi',
    'User click on cool on kyun karni hai beesi',
    'User click on back button on kyun karni hai beesi',
    'User click on beesi kya hai',
    'User land on beesi kya hai screen',
    'User click on play button on beesi kya hai screen',
    'User click on pause button on sampling UI video',
    'User didnt watch the video',
    'User watched the video for 1-10 seconds',
    'User watched the video for 11-30 seconds',
    'User watched the video for 31-60 seconds',
    'User watched the video for 61-120 seconds',
    'User watched the video for more than 120 seconds',
    'User click on back button on beesi kya hai screen',
    'User click on create beesi group on beesi kya hai screen'
]

# Reorder columns, keeping only those that exist in the data
pivot_df = pivot_df.reindex(columns=[col for col in column_order if col in pivot_df.columns])

# Format the pivot table
pivot_df = pivot_df.sort_values('event_date', ascending=False)
pivot_df['event_date'] = pivot_df['event_date'].dt.strftime('%Y-%m-%d')
pivot_df = pivot_df.set_index('event_date')
pivot_df.index.name = 'Dates'

# Display the pivot table
st.dataframe(
    pivot_df.style
    .format({col: '{:.0f}' for col in ['Total Users', 'Logged-in Users']})
    .format({col: '{:.2f}%' for col in pivot_df.columns if col not in ['Total Users', 'Logged-in Users']})
    .set_properties(**{'text-align': 'right'})
    .set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'left')]},
        {'selector': 'td', 'props': [('text-align', 'right')]},
    ]),
    use_container_width=True,
    height=600
)

# Download button
csv = pivot_df.to_csv(index=True)
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="beesi_app_user_analytics.csv",
    mime="text/csv",
)