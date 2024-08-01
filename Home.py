import streamlit as st

st.set_page_config(page_title="Home", page_icon="📊", initial_sidebar_state="collapsed")

# Custom CSS for styling
st.markdown("""
<style>
    .stExpander {
        background-color: #87CEEB;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    .stExpander > div:first-child {
        background-color: #87CEEB;
        color: black;
        font-weight: bold;
        font-size: 18px;
        padding: 10px;
        border-radius: 10px 10px 0 0;
    }
    .stExpander > div:nth-child(2) {
        background-color: #F0F0F0;
        border-radius: 0 0 10px 10px;
        padding: 20px;
    }
    .stButton>button {
        width: 100%;
        background-color: white;
        color: black;
        border: 2px solid #e74c3c;
        border-radius: 10px;
        padding: 10px;
        font-size: 16px;
        cursor: pointer;
        transition: background-color 0.3s, color 0.3s;
        margin-bottom: 10px;
    }
    .stButton>button:hover {
        background-color: #e74c3c;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

def create_button(button_name, page_path):
    if st.button(button_name):
        st.switch_page(page_path)

def main():
    st.title("Home Page")

    # Web Analytics Pages
    with st.expander("Web Analytics Pages", expanded=True):
        create_button("Web App All Users", "pages/7_WebApp_All_Users.py")
        create_button("Scroll Depth Analytics", "pages/6_Scroll_Depth_Analytics.py")

    # Android App Analytics Pages
    with st.expander("Android App Analytics Pages", expanded=False):
        create_button("Overview", "pages/5_Android_App_Overview.py")
        create_button("Explore Journey", "pages/4_Android_App_Explore_Journey.py")
        create_button("Total User Onboarding", "pages/3_TotalUsers_App_Onboarding_Journey.py")
        create_button("New User Onboarding", "pages/2_NewUser_App_Onboarding_Journey.py")

if __name__ == "__main__":
    main()