import streamlit as st
import pandas as pd
import plotly.express as px
from utils.plots import *

st.set_page_config(page_title="Single station analysis", layout="wide")
idx = st.selectbox(
    "Station:", get_unique_stations(), index=None, placeholder="Select a station..."
)

if idx:
    st.plotly_chart(time_series_rides(idx))

bottom_left, bottom_right = st.columns([1, 1], border=True)
with bottom_left:
    st.plotly_chart(rides_map(idx))
with bottom_right:
    st.plotly_chart(duration_plot(idx))
