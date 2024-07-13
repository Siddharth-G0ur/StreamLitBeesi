import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="New Users", page_icon="📊")

st.title("New Users Analytics")

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
    t1.Dates,
    t1.Event_Name,
    t1.Device,
    t1.Country,
    t1.Region,
    t1.City,
    COUNT(DISTINCT t2.User_ID) as New_Users
FROM
    `swap-vc-prod.analytics_325691371.WebApp_UserData` AS t1
INNER JOIN 
    `swap-vc-prod.analytics_325691371.New_User` AS t2
ON
    t1.User_ID = t2.User_ID and t1.Dates = t2.Dates
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
GROUP BY
    1,2,3,4,5,6
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
    values='New_Users',
    index='Dates',
    columns='Event_Name',
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

st.dataframe(pivot_df.style.format('{:.0f}', subset=pivot_df.columns[1:]), use_container_width=True)

csv = pivot_df.to_csv(index=False)
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="new_users_data.csv",
    mime="text/csv",
)