import pandas as pd
from db_tables import *

df = pd.DataFrame(
    [
        {"gameweek_id": 1, "team": "Chelsea", 'game':3},
        {"gameweek_id": 1, "team": "Brentford", 'game':4},
        {"gameweek_id": 2, "team": "Chelsea", 'game':1},
    ]
)

df = df.groupby(["team", "gameweek_id"]).count().unstack(fill_value=0).stack()
print(df)
