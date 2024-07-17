import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time
from datetime import datetime

@st.cache_data(ttl=3600)
def load_data():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
    query = """
    SELECT
        t1.Dates,
        t1.Event_Name,
        t1.Device,
        t1.Country,
        t1.Region,
        t1.City,
        COUNT(DISTINCT t1.User_ID) as New_Users,
        (CASE 
            WHEN t1.Dates = t2.Dates THEN "New User"
            ELSE "Returning User"
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
    GROUP BY
        1,2,3,4,5,6,8
    """
    return client.query(query).to_dataframe()

st.set_page_config(layout="wide")
st.title('Web App User Data Analysis')

with st.spinner('Loading data...'):
    start_time = time.time()
    data = load_data()
    end_time = time.time()

data['Dates'] = pd.to_datetime(data['Dates']).dt.strftime('%Y-%m-%d')

st.sidebar.header('Filters')

min_date = pd.to_datetime(data['Dates'].min())
max_date = pd.to_datetime(data['Dates'].max())
date_range = st.sidebar.date_input('Select Date Range', [min_date, max_date], min_value=min_date, max_value=max_date)

user_types = sorted(data['User_Type'].unique().tolist())
devices = sorted(data['Device'].unique().tolist())
countries = sorted(data['Country'].unique().tolist())
regions = sorted(data['Region'].unique().tolist())
cities = sorted(data['City'].unique().tolist())

selected_devices = st.sidebar.multiselect('Device', devices)
selected_countries = st.sidebar.multiselect('Country', countries)
selected_regions = st.sidebar.multiselect('Region', regions)
selected_cities = st.sidebar.multiselect('City', cities)
selected_user_types = st.sidebar.multiselect('User Type', user_types)

if len(date_range) == 2:
    start_date, end_date = [d.strftime('%Y-%m-%d') for d in date_range]
    data = data[(data['Dates'] >= start_date) & (data['Dates'] <= end_date)]

if selected_devices:
    data = data[data['Device'].isin(selected_devices)]
if selected_countries:
    data = data[data['Country'].isin(selected_countries)]
if selected_regions:
    data = data[data['Region'].isin(selected_regions)]
if selected_cities:
    data = data[data['City'].isin(selected_cities)]
if selected_user_types:
    data = data[data['User_Type'].isin(selected_user_types)]

pivoted_data = data.pivot_table(
    values='New_Users',
    index=['Dates'],
    columns='Event_Name',
    aggfunc='sum',
    fill_value=0
)

pivoted_data = pivoted_data.sort_index(ascending=False)

column_order = [
    'home_page_view',
    'Open_App_Appstore',
    'Open_App_Playstore',
    'Open_App_Yes_But_Kaise',
    'Open_App_Haan_Dost_Hain',
    'Open_App_Nudge_1',
    'Open_App_Nudge_2',
    'Open_App_Nudge_Floating',
    'Open_App_Whatsapp_Share_App_With_Friends'
]

pivoted_data = pivoted_data.reindex(columns=column_order)

styled_data = pivoted_data.style.format('{:,.0f}')
st.dataframe(styled_data.set_properties(**{'text-align': 'center'})
             .set_table_styles([
                 {'selector': 'th', 'props': [('background-color', '#f0f2f6'), ('color', 'black'), ('font-weight', 'bold')]},
                 {'selector': 'td', 'props': [('background-color', 'white'), ('color', 'black')]},
             ]),
             height=600, width=1200)

csv = pivoted_data.to_csv(index=True).encode('utf-8')
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="web_app_user_data.csv",
    mime="text/csv",
)