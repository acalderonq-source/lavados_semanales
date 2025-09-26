# health.py
import streamlit as st, sys, os
st.set_page_config(page_title="Health", layout="wide")
st.title("Render OK ✅")
st.write("Python:", sys.version)
st.write("Working dir:", os.getcwd())
st.write("Env PORT:", os.getenv("PORT"))
