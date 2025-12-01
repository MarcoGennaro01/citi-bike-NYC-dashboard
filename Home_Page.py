import streamlit as st
import pandas as pd
import plotly.express as px
from utils.plots import *


st.set_page_config(page_title="Home page", layout="wide")
st.markdown(
    """<style>
header.stAppHeader {background-color: transparent;}
section.stMain .block-container {padding-top: 3; z-index: 1;}
</style>""",
    unsafe_allow_html=True,
)

top_left_column, top_right_column = st.columns([2, 1], border=True)

with top_left_column:
    tab1m, tab2m = st.tabs(["Map", "Table"])
    with tab1m:
        search_term = st.text_input("Station name", "")
        st.plotly_chart(rides_map(search_term))
    with tab2m:
        stations_freq_table()
with top_right_column:
    weather_data = weather_plots()
    idx = st.selectbox(
        "Weather:",
        (0, 1, 2),
        format_func=lambda x: ["Precipitation", "Temperature", "Wind Speed"][x],
    )
    st.plotly_chart(weather_data[idx])
down_left_column, down_right_column = st.columns([2, 1], border=True)

with down_left_column:
    tab1, tab2, tab3 = st.tabs(["Duration", "Hourly", "Monthly"])
    with tab1:
        st.plotly_chart(duration_plot())
    with tab2:
        st.plotly_chart(freq_hour_plot())
    with tab3:
        st.plotly_chart(freq_month_plot())

with down_right_column:
    pies = pies_plot()
    tab1p, tab2p = st.tabs(["Member", "Electric"])
    with tab1p:
        st.plotly_chart(pies[0])
    with tab2p:
        st.plotly_chart(pies[1])
