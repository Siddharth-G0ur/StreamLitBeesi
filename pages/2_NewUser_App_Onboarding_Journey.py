import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="Android App New User Events Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

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

st.title("Android App New User Events Dashboard")

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
  new_user AS (
  SELECT
    event_date,
    user_pseudo_id,
    CASE
      WHEN ( SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'previous_first_open_count') > 0 THEN 'reinstall'
      ELSE 'fresh_install'
  END
    AS install_type
  FROM
    `swap-vc-prod.analytics_325691371.events_*`
  WHERE
    event_name = 'first_open'
    AND platform = 'ANDROID'),
  custom_events AS (
  SELECT
    user_pseudo_id,
    event_date,
    event_name,
    ( SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'screen_name') AS screen_name,
    geo.country AS Country,
    geo.region AS Region,
    geo.city AS City,
    app_info.version AS App_Version,
    device.operating_system_version AS OS_Version,
  FROM
    `swap-vc-prod.analytics_325691371.events_*`
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
      'home_screen' ))
SELECT
  n.event_date,
  n.user_pseudo_id,
  n.install_type,
  c.event_name,
  c.screen_name, 
  c.App_Version,
  c.OS_Version,
  c.Country,
  c.Region,
  c.City,
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
  new_user AS n
LEFT JOIN
  custom_events AS c
ON
  n.user_pseudo_id = c.user_pseudo_id
  AND n.event_date = c.event_date
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
        start_date = st.date_input("Start Date", value=df['event_date'].min().date(), key='event_start_date')
        end_date = st.date_input("End Date", value=df['event_date'].max().date(), key='event_end_date')

# Version Type Filter
with col2:
    with st.expander("Version Type Filter", expanded=True):
        os_version_options = clean_options(df['OS_Version'].unique())
        os_version_filter = st.multiselect('OS Version', options=os_version_options, key='os_version_filter')
        
        app_version_options = clean_options(df['App_Version'].unique())
        app_version_filter = st.multiselect('App Version', options=app_version_options, key='app_version_filter')

# Install Type Filter (in a new row)
with st.expander("Install Type Filter", expanded=True):
    install_type_options = clean_options(df['install_type'].unique())
    install_type_filter = st.multiselect('Install Type', options=install_type_options, key='install_type_filter')

# Location Filter (in a new row)
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
df = df[(df['event_date'].dt.date >= start_date) & (df['event_date'].dt.date <= end_date)]

if install_type_filter:
    df = df[df['install_type'].isin(install_type_filter)]
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

# After applying filters
pivot_df = df.pivot_table(
    values='user_pseudo_id',
    index='event_date',
    columns='Descriptive_Event',
    aggfunc='nunique',
    fill_value=0
)

# Calculate Total Users correctly
pivot_df['Total Users'] = df.groupby('event_date')['user_pseudo_id'].nunique()

# Calculate percentages
for col in pivot_df.columns:
    if col != 'Total Users':
        pivot_df[f'{col} (%)'] = pivot_df[col] / pivot_df['Total Users'] * 100

# Select percentage columns and Total Users
percentage_cols = [col for col in pivot_df.columns if '(%)' in col or col == 'Total Users']
pivot_df = pivot_df[percentage_cols]

# Rename columns
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


st.header("Documentation")

with st.expander("Dashboard Documentation"):
    st.markdown("""
    ### Dashboard Overview
    This dashboard provides insights into the onboarding journey of new users in the Android app, focusing on their first-time interactions.

    ### Columns in the Dashboard:
    1. **Dates**: The date of the events.
    2. **Total Users**: Total number of unique new users for the given date.
    3. **Splash**: Percentage of new users who viewed the splash screen.
    4. **Initial login screen**: Percentage of new users who viewed the initial login screen.
    5. **User click on login button**: Percentage of new users who clicked the login button.
    6. **Login screen load**: Percentage of new users who loaded the login screen.
    7. **User click on continue button after login screen**: Percentage of new users who clicked continue after login.
    8. **Land on OTP screen**: Percentage of new users who reached the OTP screen.
    9. **User click on continue button after otp**: Percentage of new users who clicked continue after OTP.
    10. **Profile Screen**: Percentage of new users who viewed the profile screen.
    11. **User click on Continue button after profile screen**: Percentage of new users who clicked continue after profile.
    12. **User land on Bina Savings tode screen**: Percentage of new users who reached the Bina Savings tode screen.
    13. **User clicks on Yes But kaise**: Percentage of new users who clicked "Yes But kaise".
    14. **User land on Dost hain Screen**: Percentage of new users who reached the Dost hain screen.
    15. **User clicks on haan dost hain**: Percentage of new users who clicked "haan dost hain".
    16. **User land on To Beesi Karo na Screen**: Percentage of new users who reached the To Beesi Karo na screen.
    17. **User clicks on nice**: Percentage of new users who clicked "nice".
    18. **User land on How Beesi Works Screen**: Percentage of new users who reached the How Beesi Works screen.
    19. **User clicks on got it btn**: Percentage of new users who clicked the "got it" button.
    20. **User lands on Home Screen**: Percentage of new users who reached the Home Screen.

    ### Metrics and Calculations
    - Primary metric: Distinct count of **user_pseudo_id** for each event.
    - Calculation: (Number of unique new users for the event / Total new Users) * 100
    - All percentages are relative to the Total Users count for each date.
    """)

with st.expander("SQL Query Documentation"):
    st.markdown("""
    ### SQL Query Overview
    This query retrieves data from Google Analytics 4 (GA4) events stored in BigQuery. It focuses on new user interactions within the Android app.

    ```sql
    WITH
      new_user AS (
      SELECT
        event_date,
        user_pseudo_id,
        CASE
          WHEN ( SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'previous_first_open_count') > 0 THEN 'reinstall'
          ELSE 'fresh_install'
      END AS install_type
      FROM `swap-vc-prod.analytics_325691371.events_*`
      WHERE event_name = 'first_open' AND platform = 'ANDROID'),
      custom_events AS (
      SELECT
        user_pseudo_id,
        event_date,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name') AS screen_name,
        geo.country AS Country,
        geo.region AS Region,
        geo.city AS City,
        app_info.version AS App_Version,
        device.operating_system_version AS OS_Version
      FROM `swap-vc-prod.analytics_325691371.events_*`
      WHERE platform = 'ANDROID' AND event_name IN ('screen_load', 'view_click')
        AND (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'screen_name')
          IN ('splash_screen', 'initial_login_screen', 'login_screen', 'verify_otp_screen', 'create_profile_screen', 
              'bina_savings_tode', 'dost_hain', 'to_beesi_karo_na', 'how_beesi_works', 'home_screen'))
    SELECT
      n.event_date,
      n.user_pseudo_id,
      n.install_type,
      c.event_name,
      c.screen_name, 
      c.App_Version,
      c.OS_Version,
      c.Country,
      c.Region,
      c.City,
      CASE
        WHEN c.event_name = 'screen_load' AND c.screen_name = 'splash_screen' THEN 'Splash'
        -- Additional cases for other events --
        ELSE 'Other: ' || c.event_name || ' - ' || c.screen_name
      END AS Descriptive_Event
    FROM new_user AS n
    LEFT JOIN custom_events AS c ON n.user_pseudo_id = c.user_pseudo_id AND n.event_date = c.event_date
    ```

    ### GA4 Events and Parameters Explanation:
    1. **first_open**: A GA4 automatically collected event that fires when a user opens an app for the first time after installation or re-installation.
    
    2. **previous_first_open_count**: A parameter of the 'first_open' event. It indicates how many times the app has been opened before. Used here to distinguish between fresh installs and reinstalls.
    
    3. **screen_load** and **view_click**: Custom events tracked in the app to monitor user navigation and interactions.
    
    4. **screen_name**: A custom parameter indicating the specific screen where an event occurred.

    ### Query Logic:
    1. **Identifying New Users (`new_user` CTE)**:
       - Uses the 'first_open' event to identify new users.
       - Determines if it's a fresh install or reinstall based on 'previous_first_open_count'.
    
    2. **Collecting User Events (`custom_events` CTE)**:
       - Gathers 'screen_load' and 'view_click' events for specific screens.
       - Collects additional context like geographical info and app version.
    
    3. **Joining and Categorizing Data (Main SELECT)**:
       - Combines new user data with their corresponding events.
       - Creates a descriptive label for each interaction for easier analysis.

    This query enables a comprehensive view of new user behavior, tracking their journey from app installation through various onboarding screens.
    """)

with st.expander("Streamlit Page Code Documentation"):
    st.markdown("""
    ### Streamlit Page Overview:
    This page visualizes GA4 data from BigQuery, focusing on new user onboarding in the Android app.

    ### Data Flow and Manipulations:

    1. **BigQuery Client Setup**:
       ```python
       credentials = service_account.Credentials.from_service_account_info(
           st.secrets["gcp_service_account"]
       )
       client = bigquery.Client(credentials=credentials)
       ```
       Explanation: Sets up a secure connection to BigQuery using service account credentials stored in Streamlit secrets.

    2. **Query Execution**:
       ```python
       @st.cache_data(ttl=3600)
       def run_query(query):
           query_job = client.query(query)
           return query_job.to_dataframe()

       df = run_query(sql_query)
       ```
       Explanation: Executes the SQL query against BigQuery and caches the result for an hour to improve performance.

    3. **Data Preprocessing**:
       ```python
       df['event_date'] = pd.to_datetime(df['event_date'], format='%Y%m%d')

       def clean_options(options):
           return sorted([opt for opt in options if opt is not None and opt != ''])
       ```
       Explanation: Converts date strings to datetime objects and defines a function to clean filter options.

    4. **Filtering Mechanism**:
       ```python
       start_date = st.date_input("Start Date", value=df['event_date'].min().date())
       end_date = st.date_input("End Date", value=df['event_date'].max().date())

       os_version_filter = st.multiselect('OS Version', options=os_version_options)
       app_version_filter = st.multiselect('App Version', options=app_version_options)
       install_type_filter = st.multiselect('Install Type', options=install_type_options)

       df = df[(df['event_date'].dt.date >= start_date) & (df['event_date'].dt.date <= end_date)]
       if install_type_filter:
           df = df[df['install_type'].isin(install_type_filter)]
       # Additional filter applications...
       ```
       Explanation: Creates interactive filters for date range, OS version, app version, and install type, then applies these filters to the dataframe.

    5. **Data Transformation and Display**:
       ```python
       pivot_df = df.pivot_table(
           values='user_pseudo_id',
           index='event_date',
           columns='Descriptive_Event',
           aggfunc='nunique',
           fill_value=0
       )

       pivot_df['Total Users'] = df.groupby('event_date')['user_pseudo_id'].nunique()

       for col in pivot_df.columns:
           if col != 'Total Users':
               pivot_df[f'{col} (%)'] = pivot_df[col] / pivot_df['Total Users'] * 100

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
       ```
       Explanation: Pivots the dataframe to show unique users per event per day, calculates percentages, and displays the result in a formatted Streamlit dataframe.

    6. **Data Download Feature**:
       ```python
       csv = pivot_df.to_csv(index=True)
       st.download_button(
           label="Download data as CSV",
           data=csv,
           file_name="new_users_events_data.csv",
           mime="text/csv",
       )
       ```
       Explanation: Provides a button to download the displayed data as a CSV file.

""")