from db_tables import *
import streamlit as st
import plotly.express as px

st.set_page_config(layout="wide")

app_context = app.app_context()
app_context.push()


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


@st.cache
def load_fixtures():
    fixtures = TeamFixtureResults().query.all()
    df = object_as_df(fixtures)
    df["team_name"] = df["team_id"].apply(map_team_names)
    df["opponent_name"] = df["opponent_id"].apply(map_team_names)
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
        ]
    ]  # set column order
    return df


@st.cache
def load_remaining_fixtures(df):
    df = df.copy()
    df = df.loc[df["finished"] == 0]
    df = df.drop(["score", "opponent_score", "win"], axis=1)
    return df


@st.cache
def load_unscheduled_fixtures(df):
    df = df.copy()
    df = df.loc[df["gameweek_id"] == 0]
    return df


@st.cache
def load_specific_fixtures(df):
    df = df.copy()
    df = df.loc[
        (df["gameweek_id"] >= fixture_start) & (df["gameweek_id"] <= fixture_end)
    ]
    return df


# loads
fixtures = load_fixtures()
remaining_fixtures = load_remaining_fixtures(fixtures)
unscheduled_fixtures = load_unscheduled_fixtures(remaining_fixtures)

t_allfixtures, t_remainingfixtures, t_unscheduledfixtures, t_chosengameweeks = st.tabs(
    ["All Fixtures", "Remaining Fixtures", "Unscheduled Fixtures", "Specific Gameweeks"]
)
with t_allfixtures:
    st.title("All Fixtures")
    st.dataframe(fixtures)
with t_remainingfixtures:
    st.title("Remaing Fixtures")
    st.dataframe(remaining_fixtures)
with t_unscheduledfixtures:
    st.title("Unscheduled Fixtures")
    st.dataframe(unscheduled_fixtures)
with t_chosengameweeks:
    fixture_start, fixture_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )
    specific_fixtures = load_specific_fixtures(fixtures)

    fixture_difficulty = (
        specific_fixtures[["team_name", "fixture_difficulty"]]
        .groupby("team_name")
        .mean()
        .sort_values("fixture_difficulty")
        .reset_index()
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(specific_fixtures)
    with col2:
        # st.dataframe(fixture_difficulty)
        fig = px.bar(
            fixture_difficulty,
            x="team_name",
            y="fixture_difficulty",
            color="team_name",
            text_auto=True,
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig)
