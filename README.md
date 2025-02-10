# Project Data Engineering Zoomcamp 2025

<details>
  <summary>Module 1 Homework</summary>

## Module 1 Docker & SQL

**Question 1. Understanding docker first run**

        docker run -it python:3.12.8 bash
        pip --version

**Prepare Postgres**

>The file import_data.jpynb in the repo records this part of the homework.
>The csv data files were directly downloaded from the webpage.
    
**Question 3. Trip Segmentation Count**

* Up to 1 mile
        
        SELECT count(*)
        FROM green_taxi
        WHERE lpep_pickup_datetime >= '2019-10-01'
        AND lpep_pickup_datetime < '2019-11-01'
        AND trip_distance <=1;

* In between 1 (exclusive) and 3 miles (inclusive)
  
        SELECT count(*)
        FROM green_taxi
        WHERE lpep_pickup_datetime >= '2019-10-01'
        AND lpep_pickup_datetime < '2019-11-01'
        AND trip_distance > 1 
        AND trip_distance <= 3;

* In between 3 (exclusive) and 7 miles (inclusive)

        SELECT count(*)
        FROM green_taxi
        WHERE lpep_pickup_datetime >= '2019-10-01'
        AND lpep_pickup_datetime < '2019-11-01'
        AND trip_distance > 3
        AND trip_distance <= 7;

* In between 7 (exclusive) and 10 miles (inclusive)

        SELECT count(*)
        FROM green_taxi
        WHERE lpep_pickup_datetime >= '2019-10-01'
        AND lpep_pickup_datetime < '2019-11-01'
        AND trip_distance > 7
        AND trip_distance <= 10;

* Over 10 miles

        SELECT count(*)
        FROM green_taxi
        WHERE lpep_pickup_datetime >= '2019-10-01'
        AND lpep_pickup_datetime < '2019-11-01'
        AND trip_distance > 10;

**Question 4. Longest trip for each day**

    SELECT lpep_pickup_datetime
    FROM green_taxi
    ORDER BY trip_distance DESC
    LIMIT 1;

**Question 5. Three biggest pickup zones**

    SELECT "Zone"
    FROM taxi_zone tz
    JOIN (
        SELECT sum(total_amount) sa,"PULocationID"
        FROM green_taxi gt
        WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
        GROIP BY 2
    ) t1
    OM tz."LocationID" = t1."PULocationID"
    WHERE t1.sa > 13000;

**Question 6. Largest tip**

    SELECT gt.lpep_pickup_datetime, tip_amount, zpu.    "Zone" as pickup_zone, zdo."Zone"  as dropoff_zone
    FROM green_taxi gt
    JOIN taxi_zone zpu 
    ON gt."PULocationID" = zpu."LocationID"
    JOIN taxi_zone zdo 
    ON gt."DOLocationID" = zdo."LocationID"
    WHERE gt.lpep_pickup_datetime >= '2019-10-01'
    AND gt.lpep_pickup_datetime < '2019-11-01'
    AND zpu."Zone" = 'East Harlem North'
    ORDER BY tip_amount DESC
    LIMIT 1;

</details>
<details>
  <summary>Module 3 Homework</summary>

## Module 3 Data Warehouse and BigQuery

**Preparation 1: Data upload to GCP**
>Used Kestra(myflow_GCP_upload.yaml) to download data from NY Taxi website and then upload them to GCP, one month after another manually. 

**Preparation 2:Set up BigQuery**

    CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp2025-448509.dezoomamp_dataset03.yellow_tripdata_2024_external`
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://dezoomcamp2025-448509-bucket03/*.parquet']
    );

    CREATE TABLE `dezoomcamp2025-448509.dezoomamp_dataset03.yellow_tripdata_2024` AS
    SELECT * FROM `dezoomcamp2025-448509.dezoomamp_dataset03.yellow_tripdata_2024_external`;

**Question 1: What is count of records for the 2024 Yellow Taxi Data?**
>Checked "DETAILS" page of the yellow_tripdata_2024 table, which was created in Preparation 2.

**Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?**

    SELECT DISTINCT PULocationID from dezoomamp_dataset03.yellow_tripdata_2024_external;

    SELECT DISTINCT PULocationID from dezoomamp_dataset03.yellow_tripdata_2024;

**Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?**

    SELECT PULocationID from dezoomamp_dataset03.yellow_tripdata_2024;

    SELECT PULocationID, DOLocationID from dezoomamp_dataset03.yellow_tripdata_2024;

**Question 4: How many records have a fare_amount of 0?**

    SELECT count(*) FROM dezoomamp_dataset03.yellow_tripdata_2024
    WHERE fare_amount = 0;

**Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)**

    CREATE OR REPLACE TABLE `dezoomcamp2025-448509.dezoomamp_dataset03.yellow_tripdata_2024_pandc`
    PARTITION BY DATE(tpep_dropoff_datetime)
    CLUSTER BY VendorID
    AS
        SELECT * FROM `dezoomamp_dataset03.yellow_tripdata_2024`;


**Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)**

    SELECT DISTINCT VendorID
    FROM `dezoomamp_dataset03.yellow_tripdata_2024`
    WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

    SELECT DISTINCT VendorID
    FROM `dezoomamp_dataset03.yellow_tripdata_2024_pandc`
    WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

**Question 9: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?**

    SELECT count(*) 
    FROM `dezoomamp_dataset03.yellow_tripdata_2024`;
>When querying the whole unpartitioned and unclustered table, the estimated bytes being processed is 0 Byte. Because the same as in question 1, BigQuery can simply use the row counts of the whole table recorded in DETAILS page of the table.

    SELECT count(*) 
    FROM `dezoomamp_dataset03.yellow_tripdata_2024_pandc`
    WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
>When querying the partitioned table and filering based on the column that is used for partioning, the Byte estimation that will be processed could drop drastically, i.e. In the above query, it is only 13.42MB. 

    SELECT count(*) 
    FROM `dezoomamp_dataset03.yellow_tripdata_2024_pandc`
    WHERE VendorID =1;

>When querying the partitioned and clustered table and filering based on the column that is used for clustering, the byte estimation is smaller than querying the whole table, but not by much.

</details>