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


@st.cache
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
def load_remaining_teamgames(df):
    df = df.copy()
    df = df.loc[df["finished"] == 0]
    df = df.drop(["score", "opponent_score", "win", "goals_scored"], axis=1)
    return df


@st.cache
def load_unscheduled_teamgames(df):
    df = df.copy()
    df = df.loc[df["gameweek_id"] == 0]
    return df


@st.cache
def load_specific_teamgames(df):
    df = df.copy()
    df = df.loc[
        (df["gameweek_id"] >= gameweek_start) & (df["gameweek_id"] <= gameweek_end)
    ]
    return df


@st.cache
def load_double_gameweeks(df):
    df = pd.DataFrame(
        df[["team_name", "gameweek_id"]]
        .groupby(["gameweek_id", "team_name"])
        .value_counts()
        .reset_index()
    )
    df.columns = ["gameweek_id", "team_name", "games_count"]
    df = df.loc[df["games_count"] > 1]
    return df


@st.cache
def load_finished_games(df):
    df = df.loc[df["finished"] == 1]
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
def team_list():
    teams = Teams().query.all()
    team_list = [team.team_name for team in teams]
    return team_list


# filters
team_names = team_list()

# team_games
teamgames = team_games()
remaining_teamgames = load_remaining_teamgames(teamgames)
unscheduled_teamgames = load_unscheduled_teamgames(remaining_teamgames)
double_gameweeks = load_double_gameweeks(remaining_teamgames)
finished_games = load_finished_games(teamgames)

with sidebar:
    fixture_teams = st.multiselect("Select Teams to Include", team_names, team_names)
    gameweek_start, gameweek_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )


c = st.container()
with c:
    st.title("Fixture List")
    game_state = st.radio(
        "Game States",
        ["Finished", "Upcoming", "Both"],
        horizontal=True,
    )
    st.dataframe(load_fixtures())
