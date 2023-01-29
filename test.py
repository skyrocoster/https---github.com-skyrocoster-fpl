import pandas as pd
from db_tables import *

current_gameweek = 22

class selections():
    def __init__(self):
        self.sel_start_gw = current_gameweek

sel = selections()
print(sel.sel_start_gw)
sel.sel_start_gw = 1
print(sel.sel_start_gw)