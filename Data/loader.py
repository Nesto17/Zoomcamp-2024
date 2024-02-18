import pandas as pd

# Assuming you have a URL to the Parquet file
# parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-{}.parquet"
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-{}.parquet"

months = ['0' + str(i) if i < 10 else str(i) for i in range(1, 13)]

# Specify the local folder where you want to store the downloaded file
local_folder = "./fhv/"

# Use pandas to read the Parquet file and save it locally
for month in months:
  df = pd.read_parquet(parquet_url.format(month))
  df.to_parquet(local_folder + "fhv_tripdata_2019-{}.parquet".format(month))
  print(f"Parquet file for month {month} downloaded and stored locally.")