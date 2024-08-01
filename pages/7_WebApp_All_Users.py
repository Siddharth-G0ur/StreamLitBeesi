import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import asyncio

st.set_page_config(page_title="WebApp User Analytics Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

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

# Set up Google Cloud credentials
def get_bigquery_client():
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials)
    except Exception as e:
        st.error(f"Failed to set up BigQuery client: {str(e)}")
        return None

client = get_bigquery_client()

if client is None:
    st.error("Unable to proceed without BigQuery client. Please check your credentials and try again.")
    st.stop()

# Asynchronous query execution
async def run_query_async(query):
    query_job = client.query(query)
    while True:
        query_job.reload()
        if query_job.state == 'DONE':
            if query_job.error_result:
                raise Exception(query_job.error_result)
            break
        await asyncio.sleep(1)
    return query_job.to_dataframe()

# Your event_query goes here (removed for brevity)
event_query = """
SELECT
    COALESCE(CAST(t1.Dates AS STRING), CAST(CURRENT_DATE() AS STRING)) AS Dates,
    t1.Event_Name,
    COALESCE(t1.Device, 'Unknown') AS Device,
    COALESCE(t1.Country, 'Unknown') AS Country,
    COALESCE(t1.Region, 'Unknown') AS Region,
    COALESCE(t1.City, 'Unknown') AS City,
    t1.User_ID as User_ID,
    (CASE 
        WHEN t1.Dates = t2.Dates THEN 'New User'
        ELSE 'Returning User'
    END) as User_Type
FROM
    `swap-vc-prod.analytics_325691371.WebApp_UserData` AS t1
LEFT JOIN 
    `swap-vc-prod.analytics_325691371.New_User` AS t2
ON
    t1.User_ID = t2.User_ID 
WHERE
    t1.Event_Name IN (
        'home_page_view',
        'Open_App_Playstore',
        'Open_App_Appstore',
        'Open_App_Yes_But_Kaise',
        'Open_App_Haan_Dost_Hain',
        'Open_App_Nudge_Floating',
        'Open_App_Nudge_1',
        'Open_App_Nudge_2',
        'Open_App_Whatsapp_Share_App_With_Friends'
    )
"""

@st.cache_data(ttl=3600) 
def get_processed_data():
    # Execute query
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    event_df = loop.run_until_complete(run_query_async(event_query))
    
    # Process data
    event_df['Dates'] = pd.to_datetime(event_df['Dates'])
    
    return event_df

# Get the processed data
event_df = get_processed_data()

# Event Analytics
st.header("WebApp Event Analytics")

# Function to clean filter options
def clean_options(options):
    return sorted([opt for opt in options if opt and str(opt).lower() not in ['none', 'unknown', 'nan']])

# Create columns for filter categories
col1, col2 = st.columns(2)

# Date Filter
with col1:
    with st.expander("Date Filter", expanded=True):
        start_date_event = st.date_input("Start Date", value=event_df['Dates'].min(), key='event_start_date')
        end_date_event = st.date_input("End Date", value=event_df['Dates'].max(), key='event_end_date')

# Other Filters
with col2:
    with st.expander("Other Filters", expanded=True):
        device_options = clean_options(event_df['Device'].unique())
        device_filter = st.multiselect('Device', options=device_options, key='device_filter')
        user_type_options = clean_options(event_df['User_Type'].unique())
        user_type_filter_event = st.multiselect('User Type', options=user_type_options, key='user_type_filter_event')

# Location Filter
with st.expander("Location Filter", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        country_options = clean_options(event_df['Country'].unique())
        country_filter = st.multiselect('Country', options=country_options, key='country_filter')
    with col2:
        region_options = clean_options(event_df['Region'].unique())
        region_filter = st.multiselect('Region', options=region_options, key='region_filter')
    with col3:
        city_options = clean_options(event_df['City'].unique())
        city_filter = st.multiselect('City', options=city_options, key='city_filter')

# Apply filters for Event Analytics
mask_event = (event_df['Dates'].dt.date >= start_date_event) & (event_df['Dates'].dt.date <= end_date_event)
if country_filter:
    mask_event &= event_df['Country'].fillna('Unknown').isin(country_filter)
if region_filter:
    mask_event &= event_df['Region'].fillna('Unknown').isin(region_filter)
if city_filter:
    mask_event &= event_df['City'].fillna('Unknown').isin(city_filter)
if device_filter:
    mask_event &= event_df['Device'].fillna('Unknown').isin(device_filter)
if user_type_filter_event:
    mask_event &= event_df['User_Type'].fillna('Unknown').isin(user_type_filter_event)

filtered_event_df = event_df[mask_event].copy()

# Process data for Event Analytics
pivot_df = filtered_event_df.pivot_table(
    values='User_ID',
    index='Dates',
    columns='Event_Name',
    aggfunc=lambda x: len(x.unique()),
    fill_value=0
)

pivot_df = pivot_df.sort_index(ascending=False)
pivot_df.index = pivot_df.index.strftime('%Y-%m-%d')
pivot_df.index.name = 'Dates'

column_order = ['home_page_view', 'Open_App_Appstore', 'Open_App_Playstore',
                'Open_App_Yes_But_Kaise', 'Open_App_Haan_Dost_Hain',
                'Open_App_Nudge_1', 'Open_App_Nudge_2', 'Open_App_Nudge_Floating',
                'Open_App_Whatsapp_Share_App_With_Friends']
pivot_df = pivot_df.reindex(columns=column_order)

# Style the dataframe
def style_dataframe(df):
    return df.style.set_properties(**{
        'background-color': '#f0f2f6',
        'color': 'black',
        'border-color': 'white',
        'text-align': 'center'
    }).set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#4e73df'), ('color', 'white')]},
        {'selector': 'tr:nth-of-type(even)', 'props': [('background-color', '#e6e9f0')]},
        {'selector': 'td', 'props': [('padding', '10px')]},
    ]).format('{:,.0f}')

styled_event_df = style_dataframe(pivot_df)

st.dataframe(styled_event_df, width=1500, height=500)

# Download button for event data
csv_event = pivot_df.to_csv(index=True)
st.download_button(
    label="Download Event Data as CSV",
    data=csv_event,
    file_name="event_results.csv",
    mime="text/csv",
)

st.header("Documentation")

with st.expander("Dashboard Documentation"):
    st.markdown("""
    ### Dashboard Overview
    This dashboard provides insights into user events on the WebApp, focusing on various interactions and user types.

    ### Columns in the Dashboard:
    1. **Dates**: The date of the events.
    2. **home_page_view**: Number of unique users who viewed the home page.
    3. **Open_App_Appstore**: Number of unique users who opened the app via the App Store.
    4. **Open_App_Playstore**: Number of unique users who opened the app via the Play Store.
    5. **Open_App_Yes_But_Kaise**: Number of unique users who interacted with the "Yes But Kaise" feature.
    6. **Open_App_Haan_Dost_Hain**: Number of unique users who interacted with the "Haan Dost Hain" feature.
    7. **Open_App_Nudge_1**: Number of unique users who interacted with Nudge 1.
    8. **Open_App_Nudge_2**: Number of unique users who interacted with Nudge 2.
    9. **Open_App_Nudge_Floating**: Number of unique users who interacted with the floating nudge.
    10. **Open_App_Whatsapp_Share_App_With_Friends**: Number of unique users who shared the app with friends via WhatsApp.

    ### Metrics and Calculations
    - Primary metric: Distinct count of **User_ID** for each event.
    - The dashboard displays a pivot table showing the count of unique users for each event type on each date.
    """)

with st.expander("SQL Query Documentation"):
    st.markdown("""
    ### SQL Query Overview
    This query retrieves WebApp user event data from BigQuery, including user types and geographical information.

    ```sql
    SELECT
        COALESCE(CAST(t1.Dates AS STRING), CAST(CURRENT_DATE() AS STRING)) AS Dates,
        t1.Event_Name,
        COALESCE(t1.Device, 'Unknown') AS Device,
        COALESCE(t1.Country, 'Unknown') AS Country,
        COALESCE(t1.Region, 'Unknown') AS Region,
        COALESCE(t1.City, 'Unknown') AS City,
        t1.User_ID as User_ID,
        (CASE 
            WHEN t1.Dates = t2.Dates THEN 'New User'
            ELSE 'Returning User'
        END) as User_Type
    FROM
        `swap-vc-prod.analytics_325691371.WebApp_UserData` AS t1
    LEFT JOIN 
        `swap-vc-prod.analytics_325691371.New_User` AS t2
    ON
        t1.User_ID = t2.User_ID 
    WHERE
        t1.Event_Name IN (
            'home_page_view',
            'Open_App_Playstore',
            'Open_App_Appstore',
            'Open_App_Yes_But_Kaise',
            'Open_App_Haan_Dost_Hain',
            'Open_App_Nudge_Floating',
            'Open_App_Nudge_1',
            'Open_App_Nudge_2',
            'Open_App_Whatsapp_Share_App_With_Friends'
        )
    ```

    ### Query Logic:
    1. Retrieves user event data from `WebApp_UserData` table.
    2. Joins with `New_User` table to determine if a user is new or returning.
    3. Filters for specific event types related to app interactions and page views.

    ### Key Attributes:
    - **Dates**: Event date, defaulting to current date if null.
    - **Event_Name**: Type of user interaction or event.
    - **Device, Country, Region, City**: User's device and location info.
    - **User_ID**: Unique identifier for each user.
    - **User_Type**: Categorizes users as 'New User' or 'Returning User'.

    This query enables analysis of user behavior, distinguishing between new and returning users across various app interactions and geographical locations.
    """)

with st.expander("Streamlit Page Code Documentation"):
    st.markdown("""
    ### Streamlit Page Overview:
    This page visualizes WebApp user event data from BigQuery, focusing on user interactions and demographics.

    ### Data Flow and Manipulations:

    1. **BigQuery Client Setup**:
       ```python
       def get_bigquery_client():
           credentials = service_account.Credentials.from_service_account_info(
               st.secrets["gcp_service_account"]
           )
           return bigquery.Client(credentials=credentials)
       ```
       Explanation: Establishes a secure connection to BigQuery using service account credentials.

    2. **Asynchronous Query Execution**:
       ```python
       async def run_query_async(query):
           query_job = client.query(query)
           while True:
               query_job.reload()
               if query_job.state == 'DONE':
                   if query_job.error_result:
                       raise Exception(query_job.error_result)
                   break
               await asyncio.sleep(1)
           return query_job.to_dataframe()
       ```
       Explanation: Executes BigQuery queries asynchronously, allowing for better performance with large datasets.

    3. **Data Retrieval and Caching**:
       ```python
       @st.cache_data(ttl=3600) 
       def get_processed_data():
           loop = asyncio.new_event_loop()
           asyncio.set_event_loop(loop)
           event_df = loop.run_until_complete(run_query_async(event_query))
           event_df['Dates'] = pd.to_datetime(event_df['Dates'])
           return event_df
       ```
       Explanation: Retrieves and processes data, caching results for an hour to improve performance.

    4. **Filtering Mechanism**:
       ```python
       def clean_options(options):
           return sorted([opt for opt in options if opt and str(opt).lower() not in ['none', 'unknown', 'nan']])

       # Date Filter
       start_date_event = st.date_input("Start Date", value=event_df['Dates'].min())
       end_date_event = st.date_input("End Date", value=event_df['Dates'].max())

       # Other Filters (Device, User Type, Location)
       device_filter = st.multiselect('Device', options=device_options)
       user_type_filter_event = st.multiselect('User Type', options=user_type_options)
       country_filter = st.multiselect('Country', options=country_options)
       # ... (similar for region and city)
       ```
       Explanation: Creates interactive filters for date range, device, user type, and location, cleaning options to remove irrelevant entries.

    5. **Data Transformation and Display**:
       ```python
       pivot_df = filtered_event_df.pivot_table(
           values='User_ID',
           index='Dates',
           columns='Event_Name',
           aggfunc=lambda x: len(x.unique()),
           fill_value=0
       )
       # ... (additional formatting)
       styled_event_df = style_dataframe(pivot_df)
       st.dataframe(styled_event_df, width=1500, height=500)
       ```
       Explanation: Pivots the dataframe to show unique users per event per day, applies custom styling, and displays in Streamlit.

    6. **Data Download Feature**:
       ```python
       csv_event = pivot_df.to_csv(index=True)
       st.download_button(
           label="Download Event Data as CSV",
           data=csv_event,
           file_name="event_results.csv",
           mime="text/csv",
       )
       ```
       Explanation: Provides a button to download the displayed data as a CSV file.

""")