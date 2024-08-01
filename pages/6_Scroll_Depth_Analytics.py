import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import asyncio

st.set_page_config(page_title="Scroll Depth Analytics Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

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


@st.cache_data(ttl=3600) 
def get_processed_data():
    # Execute query
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    scroll_df = loop.run_until_complete(run_query_async(scroll_query))
    
    # Process data
    scroll_df['Dates'] = pd.to_datetime(scroll_df['Dates'])
    
    return scroll_df

# Get the processed data
scroll_df = get_processed_data()

# Function to clean filter options
def clean_options(options):
    return sorted([opt for opt in options if opt and str(opt).lower() not in ['none', 'unknown', 'nan']])

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
    df = df.copy()
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

st.dataframe(styled_scroll_df, use_container_width=True, height=500)

# Download button for scroll depth data
scroll_csv = scroll_pivot.reset_index().to_csv(index=False)
st.download_button(
    label="Download Scroll Depth Data as CSV",
    data=scroll_csv,
    file_name="scroll_depth_results.csv",
    mime="text/csv",
)

st.header("Documentation")

with st.expander("Dashboard Documentation"):
    st.markdown("""

    This dashboard provides insights into user scroll behavior on the WebApp.

    ### Columns in the Dashboard:
    1. **Dates**: The date of the events.
    2. **Total Users**: Total number of unique users for the given date.
    3. **Interacted Users**: Number of users who scrolled at least once.
    4. **Bounce%**: Percentage of users who did not scroll at all.
    5. **≥20%**: Percentage of users who scrolled at least 20% of the page.
    6. **≥40%**: Percentage of users who scrolled at least 40% of the page.
    7. **≥60%**: Percentage of users who scrolled at least 60% of the page.
    8. **≥80%**: Percentage of users who scrolled at least 80% of the page.
    9. **100%**: Percentage of users who scrolled 100% of the page.

    ### Metrics
    The primary metric used is the distinct count of **User_ID** for each scroll depth category.

    ### Calculations
    - Bounce% = ((Total Users - Interacted Users) / Total Users) * 100
    - Scroll depth percentages = (Users who scrolled to that depth / Total Users) * 100
    """)

with st.expander("SQL Query Documentation"):
    st.markdown("""
    ### Query:
    ```sql
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
    ```

    ### Attributes:
    1. **Dates**: The date of the event, defaulting to the current date if null.
    2. **User_ID**: Unique identifier for each user.
    3. **User_Type**: Categorizes users as 'New User' or 'Returning User'.
    4. **max_scroll_percent**: The maximum scroll percentage for each user on each date.

    ### Query Logic:
    - The query uses two CTEs (Common Table Expressions): t1 and t2.
    - t1 retrieves user data and event information from WebApp_UserData table.
    - t2 calculates the maximum scroll percentage for each user on each date.
    - The main SELECT joins these two CTEs to combine user information with scroll depth data.
    """)