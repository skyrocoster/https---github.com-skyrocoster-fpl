import requests
import json
import pandas as pd
from db_tables import *
from collections import ChainMap
import os
from pathlib import Path

fpl_api = "https://fantasy.premierleague.com/api/"
raw_extract = "data/raw_extract/"

# Team Results

fixtures = Fixtures().query.all()

team_fixtures = []
for fixture in fixtures:
    home_team = {}
    away_team = {}

    home_team["team_id"] = fixture.team_h
    away_team["team_id"] = fixture.team_a
    home_team["opponent_id"] = fixture.team_a
    away_team["opponent_id"] = fixture.team_h
    home_team["home"] = 1
    away_team["home"] = 0
    home_team["fixture_id"] = fixture.fixture_id
    away_team["fixture_id"] = fixture.fixture_id
    home_team["score"] = fixture.team_h_score
    away_team["score"] = fixture.team_a_score
    home_team["opponent_score"] = fixture.team_a_score
    away_team["opponent_score"] = fixture.team_h_score

    team_fixtures.append(home_team)
    team_fixtures.append(away_team)

for fixture in team_fixtures:
    db.session.merge(TeamFixtureResults(**fixture))
db.session.commit()
