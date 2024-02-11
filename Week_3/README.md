-- Create an external table
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data`
OPTIONS (
format = 'PARQUET',
uris = ['gs://mage-zoomcamp-ernestsalim/2022_green_taxi_data/green_tripdata_2022-*.parquet']
);

-- Create a partitioned table
CREATE OR REPLACE TABLE `zoomcamp2024-412718.ny_taxi.partitioned_2022_green_taxi_data`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT *
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;

-- Create a materialized table
CREATE OR REPLACE TABLE `zoomcamp2024-412718.ny_taxi.materialized_2022_green_taxi_data` AS
SELECT *
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;

-- Queries!!!

-- Question 1

SELECT COUNT(1)
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;

-- Question 2

SELECT COUNT(DISTINCT PULocationID)
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data;

SELECT COUNT(DISTINCT PULocationID)
FROM zoomcamp2024-412718.ny_taxi.materialized_2022_green_taxi_data;

-- Question 3

SELECT COUNT(1)
FROM zoomcamp2024-412718.ny_taxi.external_2022_green_taxi_data
WHERE fare_amount = 0;

-- Question 5

SELECT DISTINCT PULocationID
FROM zoomcamp2024-412718.ny_taxi.partitioned_2022_green_taxi_data
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM zoomcamp2024-412718.ny_taxi.materialized_2022_green_taxi_data
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
