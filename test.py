import pandas as pd
from db_tables import *

current_gameweek = 22

test = {"Hello": 1, "Goodbye": 2}
df = pd.DataFrame(test, index=[0])
melt = pd.DataFrame(test, index=[0]).melt()


class FutureFixures:

    def __init__(self, df):
        self.df = df
        self.melt = df.melt()


# print(df)
# print(melt)
test = FutureFixures(df)
print(test.df)
print(test.melt)
