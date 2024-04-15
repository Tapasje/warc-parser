import pandas as pd
import csv

try:
    df = pd.read_csv('../data/processed/html_data.csv', delimiter='\t', quoting=csv.QUOTE_NONNUMERIC)
except pd.errors.ParserError as e:
    print(f"ParserError: {e}")

print(df.head())
print(df.info())