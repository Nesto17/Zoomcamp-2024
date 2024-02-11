-- Create an external table

```sql
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-ernestsalim/2022_green_taxi_data/green_tripdata_2022-*.parquet']
);
```

-- Create a materialized table

```sql
CREATE OR REPLACE TABLE `zoomcamp2024-412718.ny_taxi.materialized_2022_green_taxi_data` AS
SELECT *
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;
```

-- Create a partitioned table

```sql
CREATE OR REPLACE TABLE `zoomcamp2024-412718.ny_taxi.partitioned_2022_green_taxi_data`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT *
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;
```

-- Queries!!!

-- Question 1

```sql
SELECT COUNT(1)
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;
```

-- Question 2

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;
```

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM zoomcamp2024-412718.ny_taxi.materialized_2022_green_taxi_data;
```

-- Question 3

```sql
SELECT COUNT(1)
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data
WHERE fare_amount = 0;
```

-- Question 5

```sql
SELECT DISTINCT PULocationID
FROM zoomcamp2024-412718.ny_taxi.partitioned_2022_green_taxi_data
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```

```sql
SELECT DISTINCT PULocationID
FROM zoomcamp2024-412718.ny_taxi.materialized_2022_green_taxi_data
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```
