from db_tables import *
import pyarrow.parquet as pq
import pyarrow as pa


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


app_context = app.app_context()
app_context.push()

data = "data/streamlit/"

# Team Games

file_name = "teamgames"

teamgames = TeamFixtureResults().query.all()
df = object_as_df(teamgames)
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
        "kickoff_time",
    ]
]  # set column order

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")

# Team Blanks

file_name = "potential_dgw"

field_rename = {"gameweek_id": "Gameweek", "kickoff_time": "Kickoff Time"}
fixtures = Fixtures().query.all()
df = object_as_df(fixtures)
df = df.loc[(df["gameweek_id"] == 0)]
df["Home Team"] = df["team_h"].apply(map_team_names)
df["Away Team"] = df["team_a"].apply(map_team_names)
df = df.rename(columns=field_rename).sort_values(["Gameweek", "Home Team"])
df = df[["Home Team", "Away Team"]]

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")

# Fixtures

file_name = "fixtures"
field_rename = {"gameweek_id": "Gameweek", "kickoff_time": "Kickoff Time"}

fixtures = Fixtures().query.all()
df = object_as_df(fixtures)

df["Home Team"] = df["team_h"].apply(map_team_names)
df["Away Team"] = df["team_a"].apply(map_team_names)

df["Home Score"] = df.apply(
    lambda x: x["team_h_score"] if x["finished"] == 1 else "-", axis=1
).astype(str)
df["Away Score"] = df.apply(
    lambda x: x["team_a_score"] if x["finished"] == 1 else "-", axis=1
).astype(str)

df = df.rename(columns=field_rename).sort_values(["Gameweek", "Home Team"])
df = df[["Gameweek", "Home Team", "Home Score", "Away Score", "Away Team", "finished"]]

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")
