import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="Android App User Events", page_icon="📊", layout="wide")

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

st.title("Android App User Events Analytics")

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
  t1 AS (
  SELECT
    event_date AS Dates,
    user_pseudo_id AS User_ID,
    event_name AS Event_Name,
    geo.country as Country,
    geo.region as Region,
    geo.city as City,
    app_info.version as App_Version,
    device.operating_system_version as OS_Version,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'screen_name') AS Screen_Name
  FROM
    `swap-vc-prod.analytics_325691371.complete_ga4_data`
  WHERE
    platform = 'ANDROID'
    AND event_name IN ('screen_load',
      'view_click')
    AND (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'screen_name') IN ( 'splash_screen',
      'initial_login_screen',
      'login_screen',
      'verify_otp_screen',
      'create_profile_screen',
      'bina_savings_tode',
      'dost_hain',
      'to_beesi_karo_na',
      'how_beesi_works',
      'home_screen') )
SELECT
  t1.Dates,
  t1.User_ID,
  t1.Country,
  t1.Region,
  t1.City,
  t1.App_Version,
  t1.OS_Version,
  CASE
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'splash_screen' THEN 'Splash'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'initial_login_screen' THEN 'Initial login screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'initial_login_screen' THEN 'User click on login button'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'login_screen' THEN 'Login screen load'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'login_screen' THEN 'User click on continue button after login screen'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'verify_otp_screen' THEN 'Land on OTP screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'verify_otp_screen' THEN 'User click on continue button after otp'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'create_profile_screen' THEN 'Profile Screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'create_profile_screen' THEN 'User click on Continue button after profile screen'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'bina_savings_tode' THEN 'User land on Bina Savings tode screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'bina_savings_tode' THEN 'User clicks on Yes But kaise'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'dost_hain' THEN 'User land on Dost hain Screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'dost_hain' THEN 'User clicks on haan dost hain'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'to_beesi_karo_na' THEN 'User land on To Beesi Karo na Screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'to_beesi_karo_na' THEN 'User clicks on nice'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'how_beesi_works' THEN 'User land on How Beesi Works Screen'
    WHEN t1.Event_Name = 'view_click' AND t1.Screen_Name = 'how_beesi_works' THEN 'User clicks on got it btn'
    WHEN t1.Event_Name = 'screen_load' AND t1.Screen_Name = 'home_screen' THEN 'User lands on Home Screen'
    WHEN t1.Event_Name IS NULL THEN 'No custom event'
    ELSE 'Other: ' || t1.Event_Name || ' - ' || t1.Screen_Name
END
  AS App_Event
FROM
  t1
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8
ORDER BY
  1 DESC
"""

df = run_query(sql_query)
df['Dates'] = pd.to_datetime(df['Dates'], format='%Y%m%d')

# Function to clean options
def clean_options(options):
    return sorted([opt for opt in options if opt is not None and opt != ''])

# Create columns for filter categories
col1, col2 = st.columns(2)

# Date Filter
with col1:
    with st.expander("Date Filter", expanded=True):
        start_date = st.date_input("Start Date", value=df['Dates'].min(), key='event_start_date')
        end_date = st.date_input("End Date", value=df['Dates'].max(), key='event_end_date')

# Other Filters
with col2:
    with st.expander("Version Filters", expanded=True):
        os_version_options = clean_options(df['OS_Version'].unique())
        os_version_filter = st.multiselect('OS Version', options=os_version_options, key='os_version_filter')
        app_version_options = clean_options(df['App_Version'].unique())
        app_version_filter = st.multiselect('App Version', options=app_version_options, key='app_version_filter')

# Location Filter
with st.expander("Location Filter", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        country_options = clean_options(df['Country'].unique())
        country_filter = st.multiselect('Country', options=country_options, key='country_filter')
    with col2:
        region_options = clean_options(df['Region'].unique())
        region_filter = st.multiselect('Region', options=region_options, key='region_filter')
    with col3:
        city_options = clean_options(df['City'].unique())
        city_filter = st.multiselect('City', options=city_options, key='city_filter')

# Apply filters
df = df[(df['Dates'].dt.date >= start_date) & (df['Dates'].dt.date <= end_date)]

if country_filter:
    df = df[df['Country'].isin(country_filter)]
if region_filter:
    df = df[df['Region'].isin(region_filter)]
if city_filter:
    df = df[df['City'].isin(city_filter)]
if os_version_filter:
    df = df[df['OS_Version'].isin(os_version_filter)]
if app_version_filter:
    df = df[df['App_Version'].isin(app_version_filter)]

# Calculate total users for each day
total_users = df.groupby('Dates')['User_ID'].nunique()

# Create pivot table
pivot_df = df.pivot_table(
    values='User_ID',
    index='Dates',
    columns='App_Event',
    aggfunc='nunique',
    fill_value=0
)

# Add Total Users column
pivot_df['Total Users'] = df.groupby('Dates')['User_ID'].nunique() 

# Calculate percentages
for col in pivot_df.columns:
    if col != 'Total Users':
        pivot_df[col] = pivot_df[col] / pivot_df['Total Users'] * 100

# Reorder columns
column_order = [
    'Total Users',
    'Splash',
    'Initial login screen',
    'User click on login button',
    'Login screen load',
    'User click on continue button after login screen',
    'Land on OTP screen',
    'User click on continue button after otp',
    'Profile Screen',
    'User click on Continue button after profile screen',
    'User land on Bina Savings tode screen',
    'User clicks on Yes But kaise',
    'User land on Dost hain Screen',
    'User clicks on haan dost hain',
    'User land on To Beesi Karo na Screen',
    'User clicks on nice',
    'User land on How Beesi Works Screen',
    'User clicks on got it btn',
    'User lands on Home Screen',
    'No custom event'
]

# Ensure all columns are present, add missing ones with 0s
for col in column_order:
    if col not in pivot_df.columns and col != 'Total Users':
        pivot_df[col] = 0

pivot_df = pivot_df.reindex(columns=[col for col in column_order if col in pivot_df.columns])

# Format the pivot table
pivot_df = pivot_df.sort_index(ascending=False)
pivot_df.index = pivot_df.index.strftime('%Y-%m-%d')
pivot_df.index.name = 'Dates'

# Display the pivot table
st.dataframe(
    pivot_df.style
    .format({col: '{:.0f}' for col in ['Total Users']})
    .format({col: '{:.2f}%' for col in pivot_df.columns if col != 'Total Users'})
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
    file_name="android_app_user_events_data.csv",
    mime="text/csv",
)