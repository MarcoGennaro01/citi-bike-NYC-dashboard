import streamlit as st
import streamlit.components.v1 as components

top_left_column, top_right_column = st.columns(2)

with top_left_column:
    file = open("./html/plot_stations_map.html")
    html_content = file.read()
    components.html(html_content, height=600)
