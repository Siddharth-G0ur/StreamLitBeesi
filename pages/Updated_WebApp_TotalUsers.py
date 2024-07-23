import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

# Set up Google Cloud credentials
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Streamlit page configuration
st.set_page_config(page_title="User Analytics", page_icon="📊", layout="wide")
st.title("User Analytics Dashboard Total Users")

# Function to run BigQuery
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

# SQL query for event data
sql_query = """
SELECT
  event_date AS Dates,
  event_name,
  device.category AS Device,
  geo.country AS Country,
  geo.region AS Region,
  geo.city as City,
  user_pseudo_id AS UserID
FROM
  `swap-vc-prod.analytics_325691371.complete_ga4_data`
WHERE
  platform = 'WEB'
  AND event_name IN ( 'home_page_view',
  'Open_App_Playstore',
  'Open_App_Appstore',
  'Open_App_Yes_But_Kaise',
  'Open_App_Haan_Dost_Hain',
  'Open_App_Nudge_Floating',
  'Open_App_Nudge_1',
  'Open_App_Nudge_2',
  'Open_App_Whatsapp_Share_App_With_Friends' )
"""

# SQL query for scroll data
scroll_query = """
SELECT
    event_date,
    user_pseudo_id,
    MAX(COALESCE((SELECT value.int_value
    FROM UNNEST(event_params)
    WHERE key = 'percent_scrolled'), 0)) AS max_scroll_percent
FROM
    `swap-vc-prod.analytics_325691371.events_*`
WHERE
    event_name = 'Scroll'
GROUP BY
    event_date, user_pseudo_id
"""

# Fetch data
df = run_query(sql_query)
df['Dates'] = pd.to_datetime(df['Dates'], format='%Y%m%d')

scroll_df = run_query(scroll_query)
scroll_df['event_date'] = pd.to_datetime(scroll_df['event_date'], format='%Y%m%d')

# Event Analytics
st.header("Event Analytics")

# Compact filters for Event Analytics
col1, col2, col3, col4 = st.columns(4)
with col1:
    start_date_event = st.date_input("Start Date", value=df['Dates'].min(), key='event_start_date')
    country_filter = st.multiselect('Country', df['Country'].unique(), key='country_filter')
with col2:
    end_date_event = st.date_input("End Date", value=df['Dates'].max(), key='event_end_date')
    region_filter = st.multiselect('Region', df['Region'].unique(), key='region_filter')
with col3:
    device_filter = st.multiselect('Device', df['Device'].unique(), key='device_filter')
with col4:
    city_filter = st.multiselect('City', df['City'].unique(), key='city_filter')

# Apply filters
mask = (df['Dates'].dt.date >= start_date_event) & (df['Dates'].dt.date <= end_date_event)
if country_filter:
    mask &= df['Country'].isin(country_filter)
if region_filter:
    mask &= df['Region'].isin(region_filter)
if city_filter:
    mask &= df['City'].isin(city_filter)
if device_filter:
    mask &= df['Device'].isin(device_filter)

filtered_df = df[mask]

# Process data for Event Analytics
pivot_df = filtered_df.pivot_table(
    values='UserID',
    index='Dates',
    columns='event_name',
    aggfunc=lambda x: len(x.unique()),
    fill_value=0
)

pivot_df = pivot_df.rename(columns={'home_page_view': 'Home'})
pivot_df = pivot_df.sort_index(ascending=False)
pivot_df.index = pivot_df.index.strftime('%Y-%m-%d')
pivot_df.index.name = 'Dates'

column_order = ['Home', 'Open_App_Appstore', 'Open_App_Playstore',
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

styled_df = style_dataframe(pivot_df)

st.dataframe(styled_df, width=1500, height=500)

# Download button for event data
csv = pivot_df.to_csv(index=True)
st.download_button(
    label="Download event data as CSV",
    data=csv,
    file_name="event_results.csv",
    mime="text/csv",
)

# Scroll Depth Analysis
st.header("Scroll Depth Analytics")

# Process scroll data
def process_scroll_data(df):
    df['event_date'] = pd.to_datetime(df['event_date']).dt.strftime('%Y-%m-%d')
    total_users = df.groupby('event_date')['user_pseudo_id'].nunique().reset_index(name='total_users')
    
    interacted_users = df[df['max_scroll_percent'] > 0].groupby('event_date')['user_pseudo_id'].nunique().reset_index(name='interacted_users')
    total_users = pd.merge(total_users, interacted_users, on='event_date', how='left')
    
    total_users['interacted_users'] = total_users['interacted_users'].fillna(0)
    
    scroll_percentages = [20, 40, 60, 80, 100]
    for percent in scroll_percentages:
        users_scrolled = df[df['max_scroll_percent'] >= percent].groupby('event_date')['user_pseudo_id'].nunique().reset_index(name=f'scrolled_{percent}')
        total_users = pd.merge(total_users, users_scrolled, on='event_date', how='left')
        total_users[f'percent_scrolled_{percent}'] = (total_users[f'scrolled_{percent}'] / total_users['total_users'] * 100).round(2)
    
    total_users = total_users.sort_values('event_date', ascending=False)
    return total_users[['event_date', 'total_users', 'interacted_users'] + [f'percent_scrolled_{p}' for p in scroll_percentages]]

scroll_pivot = process_scroll_data(scroll_df)

# Merge with total users data
total_users_df = df.groupby('Dates')['UserID'].nunique().reset_index()
total_users_df['Dates'] = pd.to_datetime(total_users_df['Dates']).dt.strftime('%Y-%m-%d')
total_users_df = total_users_df.rename(columns={'UserID': 'Total_Users'})

scroll_pivot = pd.merge(total_users_df, scroll_pivot, left_on='Dates', right_on='event_date', how='left')

# Calculate bounce percentage
scroll_pivot['Bounce%'] = ((scroll_pivot['Total_Users'] - scroll_pivot['interacted_users']) / scroll_pivot['Total_Users'] * 100).round(2)

# Reorder and rename columns
column_order = ['Total_Users', 'interacted_users', 'Bounce%', 'percent_scrolled_20', 'percent_scrolled_40', 'percent_scrolled_60', 'percent_scrolled_80', 'percent_scrolled_100']
column_names = {'interacted_users': 'Interacted Users', 'percent_scrolled_20': '≥20%', 'percent_scrolled_40': '≥40%', 'percent_scrolled_60': '≥60%', 'percent_scrolled_80': '≥80%', 'percent_scrolled_100': '100%'}
scroll_pivot = scroll_pivot[['Dates'] + column_order].rename(columns=column_names)

# Set 'Dates' as index and sort
scroll_pivot.set_index('Dates', inplace=True)
scroll_pivot = scroll_pivot.sort_index(ascending=False)
scroll_pivot = scroll_pivot.fillna(0)

# Compact filters for Scroll Depth Analysis
col1, col2 = st.columns(2)
with col1:
    start_date_scroll = st.date_input("Start Date", value=pd.to_datetime(scroll_pivot.index.min()), key='scroll_start_date')
with col2:
    end_date_scroll = st.date_input("End Date", value=pd.to_datetime(scroll_pivot.index.max()), key='scroll_end_date')

# Apply date filter
mask = (scroll_pivot.index >= start_date_scroll.strftime('%Y-%m-%d')) & (scroll_pivot.index <= end_date_scroll.strftime('%Y-%m-%d'))
scroll_pivot_filtered = scroll_pivot.loc[mask]

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
        'Total_Users': '{:,.0f}',
        'Interacted Users': '{:,.0f}',
        'Bounce%': '{:.2f}%',
        '≥20%': '{:.2f}%',
        '≥40%': '{:.2f}%',
        '≥60%': '{:.2f}%',
        '≥80%': '{:.2f}%',
        '100%': '{:.2f}%'
    })

styled_scroll_df = style_scroll_dataframe(scroll_pivot_filtered)

st.dataframe(styled_scroll_df, width=1500, height=500)

# Download button for scroll depth data
scroll_csv = scroll_pivot_filtered.reset_index().to_csv(index=False)
st.download_button(
    label="Download scroll depth data as CSV",
    data=scroll_csv,
    file_name="scroll_depth_results.csv",
    mime="text/csv",
)