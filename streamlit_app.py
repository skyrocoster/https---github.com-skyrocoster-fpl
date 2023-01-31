from db_tables import *
import streamlit as st
import plotly.express as px
import numpy as np
import pyarrow.parquet as pq

st.set_page_config(layout="wide")
sidebar = st.sidebar

app_context = app.app_context()
app_context.push()

data = "data/streamlit/"


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


def team_games():
    df = pq.read_table(f"{data}teamgames.parquet").to_pandas()
    df = df.loc[df["team_name"].isin(fixture_teams)]
    return df


def load_fixtures():
    df = pq.read_table(f"{data}fixtures.parquet").to_pandas()
    df = df.loc[
        (df["Home Team"].isin(fixture_teams)) | (df["Away Team"].isin(fixture_teams))
    ]
    df = df.loc[(df["Gameweek"] >= gameweek_start) & (df["Gameweek"] <= gameweek_end)]
    if game_state == "Finished":
        df = df.loc[df["finished"] == 1]
    elif game_state == "Upcoming":
        df = df.loc[df["finished"] == 0]
    else:
        df = df
    df = df[["Gameweek", "Home Team", "Home Score", "Away Score", "Away Team"]]
    return df


@st.cache
def load_double_gameweeks(df):
    df_dgw = df.copy()
    df_dgw = df_dgw.loc[df_dgw["gameweek_id"] != 0]
    df_dgw = df_dgw.loc[
        (df_dgw["gameweek_id"] >= gameweek_start)
        & (df_dgw["gameweek_id"] <= gameweek_end)
    ]
    if game_state == "Finished":
        df_dgw = df_dgw.loc[df_dgw["finished"] == 1]
    elif game_state == "Upcoming":
        df_dgw = df_dgw.loc[df_dgw["finished"] == 0]
    else:
        df_dgw = df_dgw
    df_dgw = pd.DataFrame(
        df_dgw[["team_name", "gameweek_id"]]
        .groupby(["gameweek_id", "team_name"])
        .value_counts()
        .reset_index()
    )
    df_dgw.columns = ["Gameweek", "Team", "games_count"]
    df_dgw = df_dgw.loc[df_dgw["games_count"] > 1]
    df_dgw = df_dgw.drop(["games_count"], axis=1)
    return df_dgw


@st.cache
def load_blank_gameweeks(df):
    df_blankgw = df.copy()
    df_blankgw = df_blankgw[["team_name", "gameweek_id", "finished"]]
    df_blankgw = df_blankgw.loc[df_blankgw["gameweek_id"] > 0]
    df_blankgw = df_blankgw.loc[
        (df_blankgw["gameweek_id"] >= gameweek_start)
        & (df_blankgw["gameweek_id"] <= gameweek_end)
    ]
    df_blankgw = (
        df_blankgw.groupby(["gameweek_id", "team_name"])
        .count()
        .unstack(fill_value=0)
        .stack()
    )
    df_blankgw = (
        df_blankgw.loc[df_blankgw["finished"] == 0]
        .reset_index()
        .drop(["finished"], axis=1)
    )
    df_blankgw.columns = ["Gameweek", "Team"]
    return df_blankgw


@st.cache
def load_potential_dgw():
    df = pq.read_table(f"{data}potential_dgw.parquet").to_pandas()
    df = df.loc[
        (df["Home Team"].isin(fixture_teams)) | (df["Away Team"].isin(fixture_teams))
    ]
    if game_state == "Finished":
        df = df.loc[df["finished"] == 1]
    elif game_state == "Upcoming":
        df = df.loc[df["finished"] == 0]
    else:
        df = df
    df = df[["Home Team", "Away Team"]]
    return df


@st.cache
def team_list():
    teams = Teams().query.all()
    team_list = [team.team_name for team in teams]
    return team_list


@st.cache
def fixture_difficulty(df):
    df_diff = df.copy()
    df_diff = df_diff.loc[
        (df_diff["gameweek_id"] >= gameweek_start)
        & (df_diff["gameweek_id"] <= gameweek_end)
    ]
    df_diff = df_diff.sort_values(["gameweek_id", "team_name"])
    return df_diff


# filters
team_names = team_list()

with sidebar:
    fixture_teams = st.multiselect("Select Teams to Include", team_names, team_names)
    gameweek_start, gameweek_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )

teamgames = team_games()

st.title("Fixtures")
st.header("Matches")
game_state = st.radio(
    "Game States",
    ["Both", "Finished", "Upcoming"],
    horizontal=True,
)
col1, col2 = st.columns(2)
with col1:
    st.markdown(
        "<h3 style='text-align: center;'>Arranged Fixtures & Results</h3>",
        unsafe_allow_html=True,
    )
    st.dataframe(load_fixtures())
with col2:
    st.markdown(
        "<h3 style='text-align: center;'>Double Gameweeks</h3>", unsafe_allow_html=True
    )
    st.dataframe(load_double_gameweeks(teamgames))
with col1:
    st.markdown(
        "<h3 style='text-align: center;'>Gameweek Blanks</h3>", unsafe_allow_html=True
    )
    st.dataframe(load_blank_gameweeks(teamgames))
with col2:
    st.markdown(
        "<h3 style='text-align: center;'>Potential Double Gameweeks</h3>",
        unsafe_allow_html=True,
    )
    st.dataframe(load_potential_dgw())

st.markdown("---")

fixture_diffs = fixture_difficulty(teamgames)
col1, col2 = st.columns(2)
with col1:
    st.header("Fixture Difficulty")
    st.dataframe(fixture_diffs)
with col2:
    tab1, tab2 = st.tabs(["Averages", "Scatter"])
    with tab1:
        avg_diff = (
            fixture_diffs[["team_name", "fixture_difficulty", "home"]]
            .groupby("team_name")
            .mean()
            .sort_values("fixture_difficulty", ascending=True)
        )
        fig = px.bar(
            avg_diff,
            x="fixture_difficulty",
            color="home",
            color_continuous_scale=[(0, "red"), (0.5, "green"), (1, "blue")],
        )
        st.plotly_chart(fig)
    with tab2:
        fig = px.scatter(
            fixture_diffs.sort_values("gameweek_id"),
            x="gameweek_id",
            y="fixture_difficulty",
            color="team_name",
        )
        fig.update_traces(
            marker=dict(size=12, line=dict(width=2, color="DarkSlateGrey")),
            selector=dict(mode="markers"),
        )
        fig.update_layout(yaxis_range=[0, 5])
        fig.update_layout(xaxis=dict(tickmode="linear", tick0=1, dtick=1))
        st.plotly_chart(fig)
