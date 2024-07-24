import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

# Set up Google Cloud credentials
try:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
except Exception as e:
    st.error(f"Failed to set up Google Cloud credentials: {str(e)}")
    st.stop()

# Streamlit page configuration
st.set_page_config(page_title="WebApp User Analytics", page_icon="📊", layout="wide")
st.title("WebApp User Analytics Dashboard")

# Function to run BigQuery
@st.cache_data(ttl=600)
def run_query(query):
    try:
        query_job = client.query(query)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"Failed to run query: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame on error

# SQL queries (use the modified versions from the previous response)
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

scroll_query = """
WITH
 t1 AS (
SELECT
    COALESCE(CAST(t1.Dates AS STRING), CAST(CURRENT_DATE() AS STRING)) AS Dates,
    t1.Event_Name,
    COALESCE(t1.Device, 'Unknown') AS Device,
    COALESCE(t1.Country, 'Unknown') AS Country,
    COALESCE(t1.Region, 'Unknown') AS Region,
    COALESCE(t1.City, 'Unknown') AS City,
    t1.User_ID AS User_ID,
    (CASE
        WHEN t1.Dates = t2.Dates THEN 'New User'
        ELSE 'Returning User'
    END) AS User_Type
FROM
    `swap-vc-prod.analytics_325691371.WebApp_UserData` AS t1
LEFT JOIN
    `swap-vc-prod.analytics_325691371.New_User` AS t2
ON
    t1.User_ID = t2.User_ID
WHERE
    t1.Event_Name IN ( 'home_page_view',
    'Open_App_Playstore',
    'Open_App_Appstore',
    'Open_App_Yes_But_Kaise',
    'Open_App_Haan_Dost_Hain',
    'Open_App_Nudge_Floating',
    'Open_App_Nudge_1',
    'Open_App_Nudge_2',
    'Open_App_Whatsapp_Share_App_With_Friends' ) ),
t2 AS (
SELECT
    COALESCE(CAST(event_date AS STRING), CAST(CURRENT_DATE() AS STRING)) AS Dates,
    user_pseudo_id AS User_ID,
    MAX(COALESCE((
    SELECT
        value.int_value
    FROM
        UNNEST(event_params)
    WHERE
        KEY = 'percent_scrolled'), 0)) AS max_scroll_percent
FROM
    `swap-vc-prod.analytics_325691371.events_*`
WHERE
    event_name = 'Scroll'
GROUP BY
    event_date,
    user_pseudo_id )
SELECT
    COALESCE(t1.Dates, CAST(CURRENT_DATE() AS STRING)) AS Dates,
    t1.User_ID,
    t1.User_Type,
    COALESCE(t2.max_scroll_percent, 0) AS max_scroll_percent
FROM
    t1
LEFT JOIN
    t2
ON
    t1.Dates = t2.Dates
AND t1.User_ID = t2.User_ID
GROUP BY
    1,
    2,
    3,
    4
ORDER BY
    1 desc
"""

# Fetch data
try:
    event_df = run_query(event_query)
    event_df['Dates'] = pd.to_datetime(event_df['Dates'])

    scroll_df = run_query(scroll_query)
    scroll_df['Dates'] = pd.to_datetime(scroll_df['Dates'])
except Exception as e:
    st.error(f"Error processing query results: {str(e)}")
    st.stop()

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

# Scroll Depth Analytics
st.header("WebApp Scroll Depth Analytics")

# Create columns for filter categories
col1, col2 = st.columns(2)

# Date Filter
with col1:
    with st.expander("Date Filter", expanded=True):
        start_date_scroll = st.date_input("Start Date", value=scroll_df['Dates'].min(), key='scroll_start_date')
        end_date_scroll = st.date_input("End Date", value=scroll_df['Dates'].max(), key='scroll_end_date')

# Other Filters
with col2:
    with st.expander("Other Filters", expanded=True):
        user_type_options_scroll = clean_options(scroll_df['User_Type'].unique())
        user_type_filter_scroll = st.multiselect('User Type', options=user_type_options_scroll, key='user_type_filter_scroll')

# Apply filters for Scroll Depth Analytics
mask_scroll = (scroll_df['Dates'].dt.date >= start_date_scroll) & (scroll_df['Dates'].dt.date <= end_date_scroll)
if user_type_filter_scroll:
    mask_scroll &= scroll_df['User_Type'].fillna('Unknown').isin(user_type_filter_scroll)

filtered_scroll_df = scroll_df[mask_scroll].copy()

# Process data for Scroll Depth Analytics
def process_scroll_data(df):
    # Create an explicit copy of the DataFrame
    df = df.copy()
    # Use .loc to modify the DataFrame
    df.loc[:, 'Dates'] = df['Dates'].fillna(pd.Timestamp.now().date())
    
    total_users = df.groupby('Dates')['User_ID'].nunique().reset_index(name='user_count')
    
    interacted_users = df[df['max_scroll_percent'] > 0].groupby('Dates')['User_ID'].nunique().reset_index(name='interacted_users')
    total_users = pd.merge(total_users, interacted_users, on='Dates', how='left')
    
    total_users['interacted_users'] = total_users['interacted_users'].fillna(0)
    total_users['bounce_percent'] = ((total_users['user_count'] - total_users['interacted_users']) / total_users['user_count'] * 100).round(2)
    
    scroll_percentages = [20, 40, 60, 80, 100]
    for percent in scroll_percentages:
        users_scrolled = df[df['max_scroll_percent'] >= percent].groupby('Dates')['User_ID'].nunique().reset_index(name=f'scrolled_{percent}')
        total_users = pd.merge(total_users, users_scrolled, on='Dates', how='left')
        total_users[f'percent_scrolled_{percent}'] = (total_users[f'scrolled_{percent}'] / total_users['user_count'] * 100).round(2)
    
    total_users = total_users.sort_values('Dates', ascending=False)
    total_users['Dates'] = total_users['Dates'].dt.strftime('%Y-%m-%d')
    return total_users

scroll_pivot = process_scroll_data(filtered_scroll_df)

# Reorder and rename columns
column_order = ['user_count', 'interacted_users', 'bounce_percent', 'percent_scrolled_20', 'percent_scrolled_40', 'percent_scrolled_60', 'percent_scrolled_80', 'percent_scrolled_100']
column_names = {'user_count': 'Total Users', 'interacted_users': 'Interacted Users', 'bounce_percent': 'Bounce%', 'percent_scrolled_20': '≥20%', 'percent_scrolled_40': '≥40%', 'percent_scrolled_60': '≥60%', 'percent_scrolled_80': '≥80%', 'percent_scrolled_100': '100%'}
scroll_pivot = scroll_pivot[['Dates'] + column_order].rename(columns=column_names)

# Set 'Dates' as index
scroll_pivot.set_index('Dates', inplace=True)

# Style the dataframe
def style_scroll_dataframe(df):
    return df.style.set_properties(**{
        'background-color': '#f0f2f6',
        'color': 'black',
        'border-color': 'white',
        'text-align': 'center'
    }).set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#4e73df'), ('color', 'white')]},
        {'selector': 'tr:nth-of-type(even)', 'props': [('background-color', '#e6e9f0')]},
        {'selector': 'td', 'props': [('padding', '10px')]},
    ]).format({
        'Total Users': '{:,.0f}',
        'Interacted Users': '{:,.0f}',
        'Bounce%': '{:.2f}%',
        '≥20%': '{:.2f}%',
        '≥40%': '{:.2f}%',
        '≥60%': '{:.2f}%',
        '≥80%': '{:.2f}%',
        '100%': '{:.2f}%'
    })

styled_scroll_df = style_scroll_dataframe(scroll_pivot)

st.dataframe(styled_scroll_df, width=1500, height=500)

# Download button for scroll depth data
scroll_csv = scroll_pivot.reset_index().to_csv(index=False)
st.download_button(
    label="Download Scroll Depth Data as CSV",
    data=scroll_csv,
    file_name="scroll_depth_results.csv",
    mime="text/csv",
)