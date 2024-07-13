# File: pages/3_AndroidApp_EventAnalysis.py

import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.title("Android App Event Analysis")

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
    t1.User_Type,
    t1.User_ID,
    t1.Install_Type,
    t1.App_Version,
    t1.OS_Version,
    t1.Country,
    t1.Region,
    t1.City,
    t1.Events, 
    t1.Users_Count, 
    CASE
        WHEN t1.User_ID = 'N/A' THEN 0
        ELSE COALESCE(t2.Total_Installs, 0)
    END AS Total_Installs
FROM 
    `swap-vc-prod.analytics_325691371.View1` t1
LEFT JOIN 
    `swap-vc-prod.beesi_google_play.Installs_Data_V1` t2
ON 
    PARSE_DATE('%Y%m%d', t1.Dates) = t2.Date
"""

df = run_query(sql_query)

# Convert Dates to datetime and set as index
df['Dates'] = pd.to_datetime(df['Dates'], format='%Y%m%d')
df.set_index('Dates', inplace=True)

# Sidebar filters
st.sidebar.header('Filters')
date_range = st.sidebar.date_input('Select Date Range', [df.index.min(), df.index.max()])
user_type_filter = st.sidebar.multiselect('Select User Type', df['User_Type'].unique())
install_type_filter = st.sidebar.multiselect('Select Install Type', df['Install_Type'].unique())
country_filter = st.sidebar.multiselect('Select Countries', df['Country'].unique())

# Apply filters
df_filtered = df[(df.index.date >= date_range[0]) & (df.index.date <= date_range[1])]
if user_type_filter:
    df_filtered = df_filtered[df_filtered['User_Type'].isin(user_type_filter)]
if install_type_filter:
    df_filtered = df_filtered[df_filtered['Install_Type'].isin(install_type_filter)]
if country_filter:
    df_filtered = df_filtered[df_filtered['Country'].isin(country_filter)]

# Calculate Total_Installs separately
total_installs = df_filtered.groupby(level=0)['Total_Installs'].max()

# Pivot the data for Users_Count
pivot_df = pd.pivot_table(df_filtered, 
                          values='Users_Count', 
                          index=df_filtered.index, 
                          columns='Events', 
                          aggfunc='sum',
                          fill_value=0)

# Add Total_Installs to the pivoted data
pivot_df['Total_Installs'] = total_installs

# Reorder columns to have Total_Installs first
cols = pivot_df.columns.tolist()
cols.insert(0, cols.pop(cols.index('Total_Installs')))
pivot_df = pivot_df[cols]

# Calculate percentages
for col in pivot_df.columns[1:]:  # Skip the Total_Installs column
    pivot_df[f"{col}_Percentage"] = pivot_df[col] / pivot_df['Total_Installs'] * 100

# Sort the DataFrame by date in descending order
pivot_df = pivot_df.sort_index(ascending=False)

# Display the pivoted DataFrame
st.dataframe(pivot_df.style.format({col: '{:.0f}' for col in pivot_df.columns if not col.endswith('Percentage')})\
                           .format({col: '{:.2f}%' for col in pivot_df.columns if col.endswith('Percentage')}),
             use_container_width=True)

# Download button
csv = pivot_df.to_csv()
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="android_app_event_analysis.csv",
    mime="text/csv",
)