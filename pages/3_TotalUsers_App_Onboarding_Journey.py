import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="Android App Total User Events Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

# if st.button("← Back to Home"):
#     st.switch_page("home.py")

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

st.title("Android App Total User Events Dashboard")

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
    event_date AS Dates,
    user_pseudo_id AS User_ID,
    geo.country as Country,
    geo.region as Region,
    geo.city as City,
    app_info.version as App_Version,
    device.operating_system_version as OS_Version,
    CASE
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'splash_screen' THEN 'Splash'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'initial_login_screen' THEN 'Initial login screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'initial_login_screen' THEN 'User click on login button'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'login_screen' THEN 'Login screen load'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'login_screen' THEN 'User click on continue button after login screen'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'verify_otp_screen' THEN 'Land on OTP screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'verify_otp_screen' THEN 'User click on continue button after otp'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'create_profile_screen' THEN 'Profile Screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'create_profile_screen' THEN 'User click on Continue button after profile screen'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'bina_savings_tode' THEN 'User land on Bina Savings tode screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'bina_savings_tode' THEN 'User clicks on Yes But kaise'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'dost_hain' THEN 'User land on Dost hain Screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'dost_hain' THEN 'User clicks on haan dost hain'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'to_beesi_karo_na' THEN 'User land on To Beesi Karo na Screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'to_beesi_karo_na' THEN 'User clicks on nice'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'how_beesi_works' THEN 'User land on How Beesi Works Screen'
      WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'how_beesi_works' THEN 'User clicks on got it btn'
      WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'home_screen' THEN 'User lands on Home Screen'
      WHEN event_name IS NULL THEN 'No custom event'
      ELSE 'Other: ' || event_name || ' - ' || (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name')
    END AS App_Event
  FROM
    `swap-vc-prod.analytics_325691371.events_*`
  WHERE
    platform = 'ANDROID'
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

# Add a default value for empty App_Events
df['App_Event'] = df['App_Event'].fillna('No Event')

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

# Define column order
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
    'No Event',
    'No custom event'
]

# Ensure all columns are present, add missing ones with 0s
for col in column_order:
    if col not in pivot_df.columns:
        pivot_df[col] = 0

# Reorder columns
pivot_df = pivot_df.reindex(columns=[col for col in column_order if col in pivot_df.columns])

# Calculate percentages
for col in pivot_df.columns:
    if col != 'Total Users':
        pivot_df[col] = (pivot_df[col] / pivot_df['Total Users']).fillna(0) * 100

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

st.header("Documentation")

with st.expander("Dashboard Documentation"):
    st.markdown("""
    This dashboard provides insights into user events on the Android app, covering all users including both new and returning users.

    ### Columns in the Dashboard:
    1. **Dates**: The date of the events.
    2. **Total Users**: Total number of unique users for the given date.
    3. **Splash**: Percentage of users who viewed the splash screen.
    4. **Initial login screen**: Percentage of users who viewed the initial login screen.
    5. **User click on login button**: Percentage of users who clicked the login button.
    6. **Login screen load**: Percentage of users who loaded the login screen.
    7. **User click on continue button after login screen**: Percentage of users who clicked continue after login.
    8. **Land on OTP screen**: Percentage of users who reached the OTP screen.
    9. **User click on continue button after otp**: Percentage of users who clicked continue after OTP.
    10. **Profile Screen**: Percentage of users who viewed the profile screen.
    11. **User click on Continue button after profile screen**: Percentage of users who clicked continue after profile.
    12. **User land on Bina Savings tode screen**: Percentage of users who reached the Bina Savings tode screen.
    13. **User clicks on Yes But kaise**: Percentage of users who clicked "Yes But kaise".
    14. **User land on Dost hain Screen**: Percentage of users who reached the Dost hain screen.
    15. **User clicks on haan dost hain**: Percentage of users who clicked "haan dost hain".
    16. **User land on To Beesi Karo na Screen**: Percentage of users who reached the To Beesi Karo na screen.
    17. **User clicks on nice**: Percentage of users who clicked "nice".
    18. **User land on How Beesi Works Screen**: Percentage of users who reached the How Beesi Works screen.
    19. **User clicks on got it btn**: Percentage of users who clicked the "got it" button.
    20. **User lands on Home Screen**: Percentage of users who reached the Home Screen.

    ### Metrics
    The primary metric used is the distinct count of **user_pseudo_id** for each event, converted to a percentage of Total Users.

    ### Calculations
    For each event: (Number of unique users for the event / Total Users) * 100
    """)

with st.expander("SQL Query Documentation"):
    st.markdown("""
    ### Query:
    ```sql
    SELECT
      event_date AS Dates,
      user_pseudo_id AS User_ID,
      geo.country as Country,
      geo.region as Region,
      geo.city as City,
      app_info.version as App_Version,
      device.operating_system_version as OS_Version,
      CASE
        WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'splash_screen' THEN 'Splash'
        WHEN event_name = 'screen_load' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'initial_login_screen' THEN 'Initial login screen'
        WHEN event_name = 'view_click' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') = 'initial_login_screen' THEN 'User click on login button'
        -- ... (other cases) ...
        WHEN event_name IS NULL THEN 'No custom event'
        ELSE 'Other: ' || event_name || ' - ' || (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name')
      END AS App_Event
    FROM
      `swap-vc-prod.analytics_325691371.complete_ga4_data`
    WHERE
      platform = 'ANDROID'
    ```

    ### Attributes:
    1. **Dates**: The date of the event.
    2. **User_ID**: Unique identifier for each user (user_pseudo_id).
    3. **Country, Region, City**: Location information of the user.
    4. **App_Version**: Version of the Android app.
    5. **OS_Version**: Version of the Android operating system.
    6. **App_Event**: Descriptive name of the event based on event_name and screen_name.

    ### Query Logic:
    - The query selects data from the complete_ga4_data table for Android platform.
    - It uses a CASE statement to categorize different events based on event_name and screen_name.
    - Location and version information are included for filtering purposes.
    """)