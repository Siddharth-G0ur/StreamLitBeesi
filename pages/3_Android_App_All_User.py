import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time
from datetime import datetime

@st.cache_data(ttl=3600)
def run_query(query):
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
    return client.query(query).to_dataframe()

# Your SQL query here (unchanged)
query = """
SELECT
  a.Dates AS Dates,
  n.user_pseudo_id,
  a.User_ID,
  CASE
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'splash_screen' THEN 'Spalsh'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'initial_login_screen' THEN 'Initial login screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'initial_login_screen' THEN 'User click on login button'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'login_screen' THEN 'Login screen load'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'login_screen' THEN 'User click on continue button after login screen'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'verify_otp_screen' THEN 'Land on OTP screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'verify_otp_screen' THEN 'User click on continue button after otp'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'create_profile_screen' THEN 'Profile Screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'create_profile_screen' THEN 'User click on Continue button after profile screen'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'bina_savings_tode' THEN 'User land on Bina Savings tode screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'bina_savings_tode' THEN 'User clicks on Yes But kaise'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'dost_hain' THEN 'User land on Dost hain Screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'dost_hain' THEN 'User clicks on haan dost hain'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'to_beesi_karo_na' THEN 'User land on To Beesi Karo na Screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'to_beesi_karo_na' THEN 'User clicks on nice'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'how_beesi_works' THEN 'User land on How Beesi Works Screen'
    WHEN a.event_name = 'view_click' AND a.screen_name = 'how_beesi_works' THEN 'User clicks on got it btn'
    WHEN a.event_name = 'screen_load' AND a.screen_name = 'home_screen' THEN 'User lands on Home Screen'
    ELSE 'Other: ' || a.event_name || ' - ' || a.screen_name
  END AS Events,
  CASE
    WHEN a.Dates = n.event_date THEN 'New User'
    ELSE 'Returning User' 
  END AS User_Type,
  n.install_type,
  a.App_Version,
  a.OS_Version,
  a.Country,
  a.Region,
  a.City,
  COUNT(DISTINCT a.User_ID) AS User_Count
FROM
  `swap-vc-prod.analytics_325691371.AppData_View` a
LEFT JOIN
  `swap-vc-prod.analytics_325691371.NewUser_App_Installs` n
ON
  a.User_ID = n.user_pseudo_id
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
ORDER BY
  1, 2, 3
"""

st.set_page_config(layout="wide")
st.title('Android App User Journey')

with st.spinner():
    start_time = time.time()
    data = run_query(query)
    end_time = time.time()

# Convert 'Dates' column to datetime and then to string in 'YYYY-MM-DD' format
data['Dates'] = pd.to_datetime(data['Dates']).dt.strftime('%Y-%m-%d')

st.sidebar.header('Filters')

# Date range filter
min_date = pd.to_datetime(data['Dates'].min())
max_date = pd.to_datetime(data['Dates'].max())
date_range = st.sidebar.date_input('Select Date Range', [min_date, max_date], min_value=min_date, max_value=max_date)

# Other filters (unchanged)
install_types = [it for it in data['install_type'].unique() if it is not None and it != '']
user_types = data['User_Type'].unique()
os_versions = data['OS_Version'].unique()
app_versions = data['App_Version'].unique()
countries = data['Country'].unique()
regions = data['Region'].unique()
cities = data['City'].unique()

selected_user_types = st.sidebar.multiselect('User Type', user_types)
selected_install_types = st.sidebar.multiselect('Install Type', install_types)
selected_os_versions = st.sidebar.multiselect('OS Version', os_versions)
selected_app_versions = st.sidebar.multiselect('App Version', app_versions)
selected_countries = st.sidebar.multiselect('Country', countries)
selected_regions = st.sidebar.multiselect('Region', regions)
selected_cities = st.sidebar.multiselect('City', cities)

# Apply filters
if len(date_range) == 2:
    start_date, end_date = [d.strftime('%Y-%m-%d') for d in date_range]
    data = data[(data['Dates'] >= start_date) & (data['Dates'] <= end_date)]

if selected_user_types:
    data = data[data['User_Type'].isin(selected_user_types)]
if selected_install_types:
    data = data[data['install_type'].isin(selected_install_types)]
if selected_os_versions:
    data = data[data['OS_Version'].isin(selected_os_versions)]
if selected_app_versions:
    data = data[data['App_Version'].isin(selected_app_versions)]
if selected_countries:
    data = data[data['Country'].isin(selected_countries)]
if selected_regions:
    data = data[data['Region'].isin(selected_regions)]
if selected_cities:
    data = data[data['City'].isin(selected_cities)]

pivoted_data = data.pivot_table(
    values='User_Count',
    index=['Dates'],
    columns='Events',
    aggfunc='sum',
    fill_value=0
).sort_index(ascending=False)

# Rest of your code remains unchanged
total_users = pivoted_data.sum(axis=1)
percentage_data = pivoted_data.div(total_users, axis=0) * 100

percentage_data = percentage_data.round(2).astype(str) + '%'
percentage_data.insert(0, 'Total Users', total_users)

# Your column_order and styling remain the same

styled_data = percentage_data.style.format({'Total Users': '{:,.0f}'})
for col in percentage_data.columns[1:]:
    styled_data = styled_data.format({col: lambda x: x})
st.dataframe(styled_data.set_properties(**{'text-align': 'center'})
             .set_table_styles([
                 {'selector': 'th', 'props': [('background-color', '#f0f2f6'), ('color', 'black'), ('font-weight', 'bold')]},
                 {'selector': 'td', 'props': [('background-color', 'white'), ('color', 'black')]},
             ]),
             height=600)

csv = percentage_data.to_csv(index=True).encode('utf-8')
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="event_funnel_analysis.csv",
    mime="text/csv",
)