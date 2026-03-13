
import requests
import pandas as pd

url = "https://banks.data.fdic.gov/api/financials"

response = requests.get(url)
data = response.json()

df = pd.json_normalize(data.get('data', []))

df.to_csv("data/fdic_benchmark.csv", index=False)
print("FDIC benchmark data saved")
