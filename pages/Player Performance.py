from db_tables import *
import streamlit as st
import plotly.express as px
import pyarrow.parquet as pq
import pyarrow as pa
import plotly.graph_objs as go

st.set_page_config(layout="wide")
sidebar = st.sidebar

app_context = app.app_context()
app_context.push()

player_avg_fields = [
    "was_home",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "selected",
    "goals_assists",
]

# player_fixture_fields = [

# ]


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
def load_player_avgs(df, keep_cols=player_avg_fields):
    df_avgs = df[keep_cols]
    df_avgs = (
        df_avgs.loc[df_avgs["minutes"] > 60]
        .groupby(["web_name"])
        .mean(numeric_only=True)
    )
    df_avgs = df_avgs.reset_index()
    return df_avgs


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

player_fixtures = load_player_fixtures()
player_fixtures = player_fixtures.sort_values(["gameweek_id", "player_id"])
player_gw_fig = px.line(
    player_fixtures,
    x="gameweek_id",
    y="gameweek_points",
    color=player_fixtures.index,
    markers=True,
)
player_gw_fig.update_layout(
    xaxis=dict(tickmode="linear", tick0=1, dtick=1),
    yaxis=dict(tickmode="linear", tick0=1, dtick=1),
)


tab1, tab2 = st.tabs(["Dataframe", "Line Plot"])
with tab1:
    st.subheader("By Fixture/Gameweek")
    st.dataframe(player_fixtures)
with tab2:

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(player_gw_fig)


player_fixtures_avg = load_player_avgs(player_fixtures)
tab1, tab2 = st.tabs(["Dataframe", "Scatter Plot"])
with tab1:
    st.subheader("Player Averages")
    st.dataframe(player_fixtures_avg)
with tab2:
    col1, col2 = st.columns(2)
    with col1:
        sel_player_avg_x = st.selectbox(
            "Select X Axis",
            player_avg_fields,
            key="sel_player_avg_x",
            index=2,
        )
    with col2:
        sel_player_avg_y = st.selectbox(
            "Select Y Axis", player_avg_fields, key="sel_player_avg_y", index=3
        )
        sel_player_avg_size = st.selectbox(
            "Select Size of Trace",
            player_avg_fields,
            key="sel_player_avg_size",
            index=22,
        )

    player_avg_scatter = px.scatter(
        player_fixtures_avg,
        x=sel_player_avg_x,
        y=sel_player_avg_y,
        color="web_name",
        size=sel_player_avg_size,
        trendline="ols",
    )
    st.plotly_chart(player_avg_scatter)
