import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

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
country_filter = st.sidebar.multiselect('Select Countries', df['Country'].unique())
region_filter = st.sidebar.multiselect('Select Regions', df['Region'].unique())
city_filter = st.sidebar.multiselect('Select Cities', df['City'].unique())
device_filter = st.sidebar.multiselect('Select Devices', df['Device'].unique())

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

st.dataframe(pivot_df.style.format('{:.0f}'), width=1500, height=500)

csv = pivot_df.to_csv(index=False)
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="pivoted_results.csv",
    mime="text/csv",
)