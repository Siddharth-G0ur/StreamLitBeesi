import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="App Goals Analytics Dashboard", page_icon="🎯", layout="wide", initial_sidebar_state="collapsed")

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

st.title("App Goals Analytics Dashboard")

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def run_query(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

sql_query = """
SELECT
 event_date,
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'beesi_user_id') AS beesi_user_id,
(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'beesi_user_gender') AS gender,
(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'beesi_user_age') AS age_range,
(SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'beesi_user_profession') AS profession,
event_name,
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'screen_name') AS screen_name,
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS sources,
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'goal_selected') AS goal_selected,
(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'is_custom_goal') AS is_custom_goal,
geo.country AS country,
geo.region AS region,
geo.city AS city,
app_info.version AS app_version,
device.operating_system_version AS os_version
FROM
`swap-vc-prod.analytics_325691371.events_*`
where platform = 'ANDROID'
and (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'beesi_user_id') is not null
"""

df = run_query(sql_query)

# Convert dates to datetime
df['event_date'] = pd.to_datetime(df['event_date'])

# Function to clean options
def clean_options(options):
    return sorted([opt for opt in options if opt is not None and opt != ''])

def create_filters(df, key_prefix):
    with st.expander("Filters", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input("Start Date", value=df['event_date'].min().date(), key=f'{key_prefix}_start_date')
            end_date = st.date_input("End Date", value=df['event_date'].max().date(), key=f'{key_prefix}_end_date')
        
        with col2:
            os_version_options = clean_options(df['os_version'].unique())
            os_version_filter = st.multiselect('OS Version', options=os_version_options, key=f'{key_prefix}_os_version')
            app_version_options = clean_options(df['app_version'].unique())
            app_version_filter = st.multiselect('App Version', options=app_version_options, key=f'{key_prefix}_app_version')
        
        col1, col2, col3 = st.columns(3)
        with col1:
            country_options = clean_options(df['country'].unique())
            country_filter = st.multiselect('Country', options=country_options, key=f'{key_prefix}_country')
        with col2:
            region_options = clean_options(df['region'].unique())
            region_filter = st.multiselect('Region', options=region_options, key=f'{key_prefix}_region')
        with col3:
            city_options = clean_options(df['city'].unique())
            city_filter = st.multiselect('City', options=city_options, key=f'{key_prefix}_city')
        
        col1, col2, col3 = st.columns(3)
        with col1:
            gender_options = clean_options(df['gender'].unique())
            gender_filter = st.multiselect('Gender', options=gender_options, key=f'{key_prefix}_gender')
        with col2:
            age_options = clean_options(df['age_range'].unique())
            age_filter = st.multiselect('Age', options=age_options, key=f'{key_prefix}_age')
        with col3:
            profession_options = clean_options(df['profession'].unique())
            profession_filter = st.multiselect('Profession', options=profession_options, key=f'{key_prefix}_profession')
    
    return start_date, end_date, os_version_filter, app_version_filter, country_filter, region_filter, city_filter, gender_filter, age_filter, profession_filter

def apply_filters(df, start_date, end_date, os_version_filter, app_version_filter, country_filter, region_filter, city_filter, gender_filter, age_filter, profession_filter):
    filtered_df = df[
        (df['event_date'].dt.date >= start_date) & 
        (df['event_date'].dt.date <= end_date)
    ]
    if os_version_filter:
        filtered_df = filtered_df[filtered_df['os_version'].isin(os_version_filter)]
    if app_version_filter:
        filtered_df = filtered_df[filtered_df['app_version'].isin(app_version_filter)]
    if country_filter:
        filtered_df = filtered_df[filtered_df['country'].isin(country_filter)]
    if region_filter:
        filtered_df = filtered_df[filtered_df['region'].isin(region_filter)]
    if city_filter:
        filtered_df = filtered_df[filtered_df['city'].isin(city_filter)]
    if gender_filter:
        filtered_df = filtered_df[filtered_df['gender'].isin(gender_filter)]
    if age_filter:
        filtered_df = filtered_df[filtered_df['age_range'].isin(age_filter)]
    if profession_filter:
        filtered_df = filtered_df[filtered_df['profession'].isin(profession_filter)]
    return filtered_df

# Goals Table
st.header("Goal Setting Statistics")

goals_filters = create_filters(df, 'goals')
goals_df = apply_filters(df, *goals_filters)

# Create a single boolean mask
mask = (
    (goals_df['event_name'] == 'view_click') & 
    (goals_df['screen_name'] == 'set_goal_bottomsheet') & 
    (goals_df['goal_selected'].notnull())
)

# Apply the mask in a single operation
goals_df = goals_df[mask]

if goals_df.empty:
    st.warning("No data available for the selected filters. Please adjust your filter criteria.")
else:
    goal_categories = [
        'iPhone', 'Gadgets', 'Electronics', 'Laptop', 'Travel', 'Luxury', 
        'Shopping', 'Jewellery', 'Savings', 'Host Party', 'Bike', 'Car', 
        'Online Education'
    ]

    def categorize_goal(row):
        if row['is_custom_goal'] == 1:
            return 'Others'
        for category in goal_categories:
            if category.lower() in row['goal_selected'].lower():
                return category
        return 'Others'

    goals_df['goal_category'] = goals_df.apply(categorize_goal, axis=1)

    # Get the latest goal for each user
    goals_df = goals_df.sort_values('event_date').groupby('beesi_user_id').last().reset_index()

    total_users = goals_df['beesi_user_id'].nunique()
    goal_counts = goals_df['goal_category'].value_counts()
    goal_percentages = (goal_counts / total_users * 100).round(2)

    pivot_df = pd.DataFrame({
        'Total Users': [total_users],
        **{goal: [percentage] for goal, percentage in goal_percentages.items()}
    })

    st.dataframe(
        pivot_df.style
        .format({'Total Users': '{:,.0f}'})
        .format({col: '{:.2f}%' for col in pivot_df.columns if col != 'Total Users'})
        .set_properties(**{'text-align': 'right'})
        .set_table_styles([
            {'selector': 'th', 'props': [('text-align', 'left')]},
            {'selector': 'td', 'props': [('text-align', 'right')]},
        ]),
        use_container_width=True,
        hide_index=True
    )

    other_goals = goals_df[goals_df['goal_category'] == 'Others']['goal_selected'].unique()
    st.subheader("Other Custom Goals")
    st.markdown("<ul>" + "".join([f"<li>{goal}</li>" for goal in other_goals]) + "</ul>", unsafe_allow_html=True)

    st.subheader("Download Data")
    goals_csv = pivot_df.to_csv(index=False)

    st.download_button(
        label="Download Goals Data as CSV",
        data=goals_csv,
        file_name="beesi_app_goal_setting_analytics.csv",
        mime="text/csv",
    )

# Sources Table
st.header("User Sources Statistics")

sources_filters = create_filters(df, 'sources')
sources_df = apply_filters(df, *sources_filters)

if sources_df.empty:
    st.warning("No data available for the selected filters. Please adjust your filter criteria.")
else:
    total_logged_in = sources_df['beesi_user_id'].nunique()
    users_with_goals = sources_df[sources_df['goal_selected'].notnull()]['beesi_user_id'].nunique()

    # Define source categories
    source_categories = {
        'Home Screen': ['set_now_home_screen', 'create_beesi_home_screen'],
        'Settings Screen': ['edit_goal_settings_screen'],
        'Group Screen': ['set_now_groups_screen', 'create_beesi_groups_screen'],
        'Reward Screen Flow': ['set_goal_reward_screen']
    }

    def categorize_source(source):
        for category, sources in source_categories.items():
            if source in sources:
                return category
        return None  

    sources_df = sources_df[sources_df['sources'].notnull()]
    sources_df['source_category'] = sources_df['sources'].apply(categorize_source)
    sources_df = sources_df[sources_df['source_category'].notnull()]  # Keep only the specified categories

    if sources_df.empty:
        st.warning("No data available for the specified source categories. Please check your data or category definitions.")
    else:
        source_counts = sources_df.groupby('source_category')['beesi_user_id'].nunique()
        source_percentages = (source_counts / users_with_goals * 100).round(2)

        sources_pivot = pd.DataFrame({
            'Total Logged-in Users': [total_logged_in],
            'Users Who Set Goals': [users_with_goals],
            **{f'{category} %': [percentage] for category, percentage in source_percentages.items()}
        })

        # Reorder columns
        column_order = ['Total Logged-in Users', 'Users Who Set Goals'] + \
                       [col for col in sources_pivot.columns if col not in ['Total Logged-in Users', 'Users Who Set Goals']]
        sources_pivot = sources_pivot[column_order]

        st.dataframe(
            sources_pivot.style
            .format({'Total Logged-in Users': '{:,.0f}', 'Users Who Set Goals': '{:,.0f}'})
            .format({col: '{:.2f}%' for col in sources_pivot.columns if col.endswith('%')})
            .set_properties(**{'text-align': 'right'})
            .set_table_styles([
                {'selector': 'th', 'props': [('text-align', 'left')]},
                {'selector': 'td', 'props': [('text-align', 'right')]},
            ]),
            use_container_width=True,
            hide_index=True
        )

        sources_csv = sources_pivot.to_csv(index=False)

        st.download_button(
            label="Download Sources Data as CSV",
            data=sources_csv,
            file_name="beesi_app_sources_analytics.csv",
            mime="text/csv",
        )