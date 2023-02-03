from db_tables import *
import streamlit as st
import plotly.express as px
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa

st.set_page_config(layout="wide")
sidebar = st.sidebar

app_context = app.app_context()
app_context.push()


@st.cache
def team_list():
    teams = Teams().query.all()
    team_list = [team.team_name for team in teams]
    return team_list


@st.cache
def load_fixture_stats():
    df_fixture_stats = pq.read_table(
        f"data/streamlit/team_performance/fixture_stats.parquet"
    ).to_pandas()
    return df_fixture_stats


fixture_stats = load_fixture_stats()
team_names = team_list()

st.dataframe(fixture_stats)

sidebar = st.sidebar
with sidebar:
    fixture_teams = st.multiselect("Select Teams to Include", team_names, team_names)
    gameweek_start, gameweek_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )
