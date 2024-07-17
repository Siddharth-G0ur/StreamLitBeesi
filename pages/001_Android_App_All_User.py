import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="New User Events", page_icon="📊", layout="wide")

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

st.title("New User Events Analytics")

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def run_query(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

sql_query = """
WITH new_users AS (
  SELECT
    user_pseudo_id,
    event_date,
    CASE
      WHEN MAX(params.value.int_value) > 0 THEN 'reinstall'
      ELSE 'fresh_install'
    END AS install_type,
    GREATEST(MAX(params.value.int_value), 0) AS number_of_reinstalls,
    MAX(app_info.version) AS App_Version,
    MAX(device.operating_system_version) AS OS_Version,
    MAX(geo.country) AS Country,
    MAX(geo.region) AS Region,
    MAX(geo.city) AS City
  FROM
    `swap-vc-prod.analytics_325691371.complete_ga4_data`,
    UNNEST(event_params) AS params
  WHERE
    event_name = 'first_open'
    AND params.key = 'previous_first_open_count'
    AND platform = 'ANDROID'
  GROUP BY
    user_pseudo_id,
    event_date
),

custom_events AS (
  SELECT
    user_pseudo_id,
    event_date,
    event_name AS Event_Name,
    (SELECT value.string_value 
     FROM UNNEST(event_params) 
     WHERE key = 'screen_name') AS Screen_Name
  FROM
    `swap-vc-prod.analytics_325691371.complete_ga4_data`
  WHERE
    platform = 'ANDROID'
    AND event_name IN ('screen_load', 'view_click')
    AND (SELECT value.string_value 
         FROM UNNEST(event_params) 
         WHERE key = 'screen_name') IN (
      'splash_screen',
      'initial_login_screen',
      'login_screen',
      'verify_otp_screen',
      'create_profile_screen',
      'bina_savings_tode',
      'dost_hain',
      'to_beesi_karo_na',
      'how_beesi_works',
      'home_screen'
    )
)

SELECT
  n.event_date,
  n.user_pseudo_id,
  n.install_type,
  n.number_of_reinstalls,
  n.App_Version,
  n.OS_Version,
  n.Country,
  n.Region,
  n.City,
  CASE
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'splash_screen' THEN 'Splash'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'initial_login_screen' THEN 'Initial login screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'initial_login_screen' THEN 'User click on login button'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'login_screen' THEN 'Login screen load'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'login_screen' THEN 'User click on continue button after login screen'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'verify_otp_screen' THEN 'Land on OTP screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'verify_otp_screen' THEN 'User click on continue button after otp'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'create_profile_screen' THEN 'Profile Screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'create_profile_screen' THEN 'User click on Continue button after profile screen'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'bina_savings_tode' THEN 'User land on Bina Savings tode screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'bina_savings_tode' THEN 'User clicks on Yes But kaise'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'dost_hain' THEN 'User land on Dost hain Screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'dost_hain' THEN 'User clicks on haan dost hain'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'to_beesi_karo_na' THEN 'User land on To Beesi Karo na Screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'to_beesi_karo_na' THEN 'User clicks on nice'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'how_beesi_works' THEN 'User land on How Beesi Works Screen'
    WHEN c.Event_Name = 'view_click' AND c.Screen_Name = 'how_beesi_works' THEN 'User clicks on got it btn'
    WHEN c.Event_Name = 'screen_load' AND c.Screen_Name = 'home_screen' THEN 'User lands on Home Screen'
    WHEN c.Event_Name IS NULL THEN 'No custom event'
    ELSE 'Other: ' || c.Event_Name || ' - ' || c.Screen_Name
  END AS Descriptive_Event
FROM
  new_users n
LEFT JOIN
  custom_events c
ON
  n.user_pseudo_id = c.user_pseudo_id
  AND n.event_date = c.event_date
"""

df = run_query(sql_query)
df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d')

st.sidebar.header('Filters')

date_range = st.sidebar.date_input('Select Date Range', [df['event_date'].min().date(), df['event_date'].max().date()])

install_type_filter = st.sidebar.multiselect('Select Install Types', df['install_type'].unique())
app_version_filter = st.sidebar.multiselect('Select App Versions', df['App_Version'].unique())
os_version_filter = st.sidebar.multiselect('Select OS Versions', df['OS_Version'].unique())
country_filter = st.sidebar.multiselect('Select Countries', df['Country'].unique())
region_filter = st.sidebar.multiselect('Select Regions', df['Region'].unique())
city_filter = st.sidebar.multiselect('Select Cities', df['City'].unique())


if len(date_range) == 2:
    start_date, end_date = date_range
    df = df[(df['event_date'].dt.date >= start_date) & (df['event_date'].dt.date <= end_date)]

if app_version_filter:
    df = df[df['App_Version'].isin(app_version_filter)]
if os_version_filter:
    df = df[df['OS_Version'].isin(os_version_filter)]
if country_filter:
    df = df[df['Country'].isin(country_filter)]
if region_filter:
    df = df[df['Region'].isin(region_filter)]
if city_filter:
    df = df[df['City'].isin(city_filter)]
if install_type_filter:
    df = df[df['install_type'].isin(install_type_filter)]

pivot_df = df.pivot_table(
    values='user_pseudo_id',
    index='event_date',
    columns='Descriptive_Event',
    aggfunc='nunique',
    fill_value=0
)

pivot_df['Total Users'] = pivot_df.sum(axis=1)

for col in pivot_df.columns:
    if col != 'Total Users':
        pivot_df[f'{col} (%)'] = pivot_df[col] / pivot_df['Total Users'] * 100

percentage_cols = [col for col in pivot_df.columns if '(%)' in col or col == 'Total Users']
pivot_df = pivot_df[percentage_cols]

pivot_df.columns = [col.replace(' (%)', '') for col in pivot_df.columns]

pivot_df = pivot_df.sort_index(ascending=False)
pivot_df.index = pivot_df.index.strftime('%Y-%m-%d')
pivot_df.index.name = 'Dates'

column_order = [
    'Dates',
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

for col in column_order:
    if col not in pivot_df.columns and col != 'Dates':
        pivot_df[col] = 0

pivot_df = pivot_df.reindex(columns=[col for col in column_order if col in pivot_df.columns])

st.dataframe(
    pivot_df.style
    .format({col: '{:.0f}' for col in ['Total Users']})
    .format({col: '{:.2f}%' for col in pivot_df.columns if col not in ['Dates', 'Total Users']})
    .set_properties(**{'text-align': 'right'})
    .set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'left')]},
        {'selector': 'td', 'props': [('text-align', 'right')]},
    ]),
    use_container_width=True,
    height=600
)

csv = pivot_df.to_csv(index=True)
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="new_users_events_data.csv",
    mime="text/csv",
)