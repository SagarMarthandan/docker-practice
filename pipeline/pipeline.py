import sys
import pandas as pd


print("arguments", sys.argv)

if len(sys.argv) < 2:
    print("Usage: pipeline.py <day>", file=sys.stderr)
    sys.exit(1)

try:
    day = int(sys.argv[1])
except ValueError:
    print(f"Error: '{sys.argv[1]}' is not a valid integer for day", file=sys.stderr)
    sys.exit(1)

print(f"Running pipeline for day {day}")


df = pd.DataFrame({
    "date": [1, 2, 3, 4, 5],
    "month": [3, 4, 5, 6, 7],
    "year": [1992, 1992, 1993, 1994, 1995]
})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")