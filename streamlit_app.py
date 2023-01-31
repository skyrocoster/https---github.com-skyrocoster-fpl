from db_tables import *
import streamlit as st
import plotly.express as px
import numpy as np

st.set_page_config(layout="wide")
sidebar = st.sidebar

app_context = app.app_context()
app_context.push()


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


def team_games():
    teamgames = TeamFixtureResults().query.all()
    df = object_as_df(teamgames)
    df["team_name"] = df["team_id"].apply(map_team_names)
    df["opponent_name"] = df["opponent_id"].apply(map_team_names)
    df["day_of_week"] = df["kickoff_time"].dt.day_name()
    # df["time"] = pd.to_datetime(df["kickoff_time"].dt.strftime("%H:%M")).dt.time
    df["goals_scored"] = df["score"] + df["opponent_score"]
    df = df[
        [
            "team_name",
            "opponent_name",
            "fixture_difficulty",
            "finished",
            "score",
            "opponent_score",
            "win",
            "home",
            "gameweek_id",
            "kickoff_time",
            "day_of_week",
            # "time",
            "goals_scored",
        ]
    ]  # set column order
    return df


@st.cache
def load_fixtures():
    field_rename = {"gameweek_id": "Gameweek", "kickoff_time": "Kickoff Time"}
    fixtures = Fixtures().query.all()
    df = object_as_df(fixtures)
    df = df.loc[
        (df["gameweek_id"] >= gameweek_start) & (df["gameweek_id"] <= gameweek_end)
    ]
    df["Home Team"] = df["team_h"].apply(map_team_names)
    df["Away Team"] = df["team_a"].apply(map_team_names)
    df["Home Score"] = df.apply(
        lambda x: x["team_h_score"] if x["finished"] == 1 else "-", axis=1
    ).astype(str)
    df["Away Score"] = df.apply(
        lambda x: x["team_a_score"] if x["finished"] == 1 else "-", axis=1
    ).astype(str)
    df = df.rename(columns=field_rename).sort_values(["Gameweek", "Home Team"])
    df = df.loc[
        (df["Home Team"].isin(fixture_teams)) | (df["Away Team"].isin(fixture_teams))
    ]
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
    df = df.copy()
    df = df.loc[df["gameweek_id"] != 0]
    df = df.loc[
        (df["gameweek_id"] >= gameweek_start) & (df["gameweek_id"] <= gameweek_end)
    ]
    df = pd.DataFrame(
        df[["team_name", "gameweek_id"]]
        .groupby(["gameweek_id", "team_name"])
        .value_counts()
        .reset_index()
    )
    df.columns = ["Gameweek", "Team", "games_count"]
    df = df.loc[df["games_count"] > 1]
    df = df.drop(["games_count"], axis=1)
    return df


@st.cache
def load_blank_gameweeks(df):
    df = df.copy()
    df = df[["team_name", "gameweek_id", "finished"]]
    df = df.loc[df["gameweek_id"] > 0]
    df = df.loc[
        (df["gameweek_id"] >= gameweek_start) & (df["gameweek_id"] <= gameweek_end)
    ]
    df = df.groupby(["gameweek_id", "team_name"]).count().unstack(fill_value=0).stack()
    df = df.loc[df["finished"] == 0].reset_index().drop(["finished"], axis=1)
    df.columns = ["Gameweek", "Team"]
    return df


@st.cache
def load_blank_teams():
    field_rename = {"gameweek_id": "Gameweek", "kickoff_time": "Kickoff Time"}
    fixtures = Fixtures().query.all()
    df = object_as_df(fixtures)
    df = df.loc[
        (df["gameweek_id"] ==0)
    ]
    df["Home Team"] = df["team_h"].apply(map_team_names)
    df["Away Team"] = df["team_a"].apply(map_team_names)
    df = df.rename(columns=field_rename).sort_values(["Gameweek", "Home Team"])
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


# filters
team_names = team_list()
teamgames = team_games()

with sidebar:
    fixture_teams = st.multiselect("Select Teams to Include", team_names, team_names)
    gameweek_start, gameweek_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )

st.title("Fixtures")
st.header("Matches")
game_state = st.radio(
    "Game States",
    ["Finished", "Upcoming", "Both"],
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
    st.dataframe(load_blank_teams())

st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    st.header("Fixture Difficulty")
