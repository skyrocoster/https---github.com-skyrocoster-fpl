import requests
import json
import pandas as pd
from db_tables import *
from collections import ChainMap
import os
from pathlib import Path

fpl_api = "https://fantasy.premierleague.com/api/"
raw_extract = "data/raw_extract/"

result = Fixtures().query.first()
print(result.teams_team_h.team_id)
# print(result.gameweeks)
