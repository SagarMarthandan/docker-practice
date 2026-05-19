import sys
import pandas as pd


print("arguments", sys.argv)

day = int(sys.argv[1])
print(f"Running pipeline for day {day}")

df = pd.DataFrame({"date": [1, 2], "month": [3, 4], "year": [1992, 1992]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")