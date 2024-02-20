import pandas as pd

# Assuming you have a URL to the Parquet file
# parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-{}.parquet"
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-{}.parquet"

months = ['0' + str(i) if i < 10 else str(i) for i in range(1, 13)]

# Specify the local folder where you want to store the downloaded file
local_folder = "./fhv/"

# Specify column dtypes (fhv)
fhv_dtypes = {
    'dispatching_base_num': 'str',
    # 'pickup_datetime': 'datetime64',
    # 'dropOff_datetime': 'datetime64',
    'SR_Flag': 'int32',
    'PUlocationID': 'int32',
    'DOlocationID': 'int32',
    'Affiliated_base_number': 'str'
}

# Use pandas to read the Parquet file and save it locally
for month in months:
  df = pd.read_parquet(parquet_url.format(month))

#   df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'], errors='coerce')
#   df['dropOff_datetime'].fillna(pd.to_datetime('1900-01-01'), inplace=True)
  df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
  df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
  
  df['PUlocationID'].fillna(0, inplace=True)
  df['PUlocationID'] = df['PUlocationID'].astype('int32')
  df['DOlocationID'].fillna(0, inplace=True)
  df['DOlocationID'] = df['DOlocationID'].astype('int32')
  df['SR_Flag'].fillna(0, inplace=True)
  df['SR_Flag'] = df['SR_Flag'].astype('int32')

  df.astype(fhv_dtypes).to_parquet(local_folder + "fhv_tripdata_2019-{}.parquet".format(month))
  print(f"Parquet file for month {month} downloaded and stored locally.")