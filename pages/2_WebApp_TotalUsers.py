import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

st.set_page_config(page_title="New Users", page_icon="📊", layout="wide")
st.title("New Users Analytics")

@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

sql_query = """
SELECT
  event_date AS Dates,
  event_name,
  device.category AS Device,
  geo.country AS Country,
  geo.region AS Region,
  geo.city as City,
  COUNT(DISTINCT user_pseudo_id) AS Users
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
GROUP BY
  1, 2, 3, 4, 5, 6
"""

df = run_query(sql_query)
df['Dates'] = pd.to_datetime(df['Dates'], format='%Y%m%d')

st.sidebar.header('Filters')

# Date range filter
date_range = st.sidebar.date_input('Select Date Range', [df['Dates'].min().date(), df['Dates'].max().date()])

country_filter = st.sidebar.multiselect('Select Countries', df['Country'].unique())
region_filter = st.sidebar.multiselect('Select Regions', df['Region'].unique())
city_filter = st.sidebar.multiselect('Select Cities', df['City'].unique())
device_filter = st.sidebar.multiselect('Select Devices', df['Device'].unique())

# Apply filters
if len(date_range) == 2:
    start_date, end_date = date_range
    df = df[(df['Dates'].dt.date >= start_date) & (df['Dates'].dt.date <= end_date)]

if country_filter:
    df = df[df['Country'].isin(country_filter)]
if region_filter:
    df = df[df['Region'].isin(region_filter)]
if city_filter:
    df = df[df['City'].isin(city_filter)]
if device_filter:
    df = df[df['Device'].isin(device_filter)]

pivot_df = df.pivot_table(
    values='Users',
    index='Dates',
    columns='event_name',
    aggfunc='sum',
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

csv = pivot_df.to_csv(index=True)
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="pivoted_results.csv",
    mime="text/csv",
)

# Add a bar chart
st.subheader("Event Trends")
chart_data = pivot_df.reset_index()
chart_data['Dates'] = pd.to_datetime(chart_data['Dates'])
chart_data = chart_data.melt('Dates', var_name='Event', value_name='Users')
st.bar_chart(chart_data.set_index('Dates'))