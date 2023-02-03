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
def load_team_list():
    teams = Teams().query.all()
    team_list = [team.team_name for team in teams]
    return team_list


@st.cache
def load_player_fixtures():
    df = pq.read_table(
        f"data/streamlit/player_performance/player_fixtures.parquet"
    ).to_pandas()
    if len(sel_teams) > 0:
        df = df.loc[df["team_name"].isin(sel_teams)]
    df = df.loc[(df["gameweek_id"] >= sel_gw_start) & (df["gameweek_id"] <= sel_gw_end)]
    if len(sel_positions) > 0:
        df = df.loc[df["position"].isin(sel_positions)]
    if len(sel_player) > 0:
        df = df.loc[df["web_name"].isin(sel_player)]
    df = df.loc[df["minutes"] > sel_minutes]
    df = df.set_index(["web_name"]).sort_values("web_name")
    if sel_home == "Home":
        df = df.loc[df["was_home"] == True]
    elif sel_home == "Away":
        df = df.loc[df["was_home"] == False]
    return df


@st.cache
def load_player_list():
    df = pq.read_table(f"data/streamlit/player_performance/players.parquet").to_pandas()
    if len(sel_teams) > 0:
        df = df.loc[df["team_name"].isin(sel_teams)]
    if len(sel_positions) > 0:
        df = df.loc[df["position"].isin(sel_positions)]
    player_list = sorted(df[["web_name"]].squeeze().tolist())
    return player_list


team_names = load_team_list()
positions = ["GKP", "FWD", "MID", "DEF"]

with sidebar:
    sel_positions = st.multiselect("Select Positions to Include", positions, positions)
    sel_teams = st.multiselect("Select Teams to Include", team_names, [])
    player_names = load_player_list()
    sel_gw_start, sel_gw_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )
    sel_player = st.multiselect("Select a Player", player_names, [])
    sel_minutes = st.slider(
        "Minimum Played Minutes", min_value=0, max_value=90, value=60
    )
    sel_home = st.radio(
        "Home or Away",
        ["Both", "Home", "Away"],
        horizontal=True,
    )

df = load_player_fixtures()
df_avg = df.loc[df["minutes"] > 60].groupby(["web_name"]).mean(numeric_only=True)
st.subheader("By Fixture")
st.dataframe(df)
st.subheader("Player Averages")
st.dataframe(df_avg)
