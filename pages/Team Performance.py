import streamlit as st
import plotly.express as px
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa

st.set_page_config(layout="wide")
sidebar = st.sidebar


@st.cache_data
def team_list():
    teams = pq.read_table(f"data/streamlit/all/teams.parquet").to_pandas()
    team_list = [team.team_name for team in teams]
    return team_list


@st.cache_data(allow_output_mutation=True)
def load_fixture_stats():
    df_fixture_stats = pq.read_table(
        f"data/streamlit/team_performance/fixture_stats.parquet"
    ).to_pandas()
    df_fixture_stats = df_fixture_stats.loc[
        (df_fixture_stats["gameweek_id"] >= sel_gw_start)
        & (df_fixture_stats["gameweek_id"] <= sel_gw_end)
    ]
    if len(sel_teams) > 0:
        df_fixture_stats = df_fixture_stats.loc[
            df_fixture_stats["team_name"].isin(sel_teams)
        ]
    if sel_home == "Home":
        df_fixture_stats = df_fixture_stats.loc[df_fixture_stats["home"] == True]
    elif sel_home == "Away":
        df_fixture_stats = df_fixture_stats.loc[df_fixture_stats["home"] == False]
    return df_fixture_stats


team_names = team_list()

fixture_stats_fields = [
    "score",
    "opponent_score",
    "team_name",
    "opponent_name",
    "total_assists",
    "total_own_goals",
    "total_clean_sheets",
    "total_saves",
    "total_yellow_cards",
    "total_red_cards",
    "total_bonus",
    "total_penalties_saved",
    "total_penalties_missed",
    "total_expected_goals",
    "avg_bps",
    "avg_influence",
    "avg_creativity",
    "avg_threat",
    "avg_ict_index",
    "avg_expected_goals",
    "avg_expected_assists",
    "avg_expected_goal_involvements",
    "avg_expected_goals_conceded",
    "avg_value",
    "fixture_number",
]

with sidebar:
    sel_teams = st.multiselect("Select Teams to Include", team_names, [])
    sel_gw_start, sel_gw_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )
    sel_home = st.radio(
        "Home or Away",
        ["Both", "Home", "Away"],
        horizontal=True,
    )

##
st.write("Figures here only apply for players that played >60 mins")


##

fixture_stats = load_fixture_stats()
st.dataframe(fixture_stats)


col1, col2 = st.columns(2)
with col1:
    sel_fixture_stats_x = st.selectbox(
        "Select X Axis",
        fixture_stats_fields,
        key="sel_fixture_stats_x",
    )
    sel_fixture_stats_color = st.selectbox(
        "Select Color of Trace",
        fixture_stats_fields,
        key="sel_fixture_stats_color",
    )
with col2:
    sel_fixture_stats_y = st.selectbox(
        "Select Y Axis", fixture_stats_fields, key="sel_fixture_stats_y", index=3
    )
    sel_fixture_stats_size = st.selectbox(
        "Select Size of Trace",
        fixture_stats_fields,
        key="sel_fixture_stats_size",
    )


fixture_stats_scatter = px.scatter(
    fixture_stats,
    x=sel_fixture_stats_x,
    y=sel_fixture_stats_y,
    size=sel_fixture_stats_size,
    color=sel_fixture_stats_color,
)
st.plotly_chart(fixture_stats_scatter)
