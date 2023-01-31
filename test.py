from time import perf_counter, sleep
from db_tables import *
import pyarrow.parquet as pq
import pyarrow as pa


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


app_context = app.app_context()
app_context.push()


def db_query():
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
    df = df.loc[df["team_name"].isin(["Arsenal", "Brentford"])]
    return df


def parquet():
    df = pq.read_table("teamgames.parquet").to_pandas()
    df = df.loc[df["team_name"].isin(["Arsenal", "Brentford"])]
    return df


start_time = perf_counter()

db_query()  # Function to measure

passed_time = perf_counter() - start_time

print(f"db_query took {passed_time}")  # It took 5.007398507999824

start_time = perf_counter()

parquet()  # Function to measure

passed_time = perf_counter() - start_time

print(f"parquet took {passed_time}")  # It took 5.007398507999824
