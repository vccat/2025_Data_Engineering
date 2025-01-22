# Project Data Engineering Zoomcamp 2005

## Module 1 Homework Docker & SQL

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
        WHERE trip_distance <=1;

* In between 1 (exclusive) and 3 miles (inclusive)
  
        SELECT count(*)
        FROM green_taxi
        WHERE trip_distance > 1 
        AND trip_distance <= 3;
* In between 3 (exclusive) and 7 miles (inclusive)

        SELECT count(*)
        FROM green_taxi
        WHERE trip_distance > 3
        AND trip_distance <= 7;
* In between 7 (exclusive) and 10 miles (inclusive)

        SELECT count(*)
        FROM green_taxi
        WHERE trip_distance > 7
        AND trip_distance <= 10;
* Over 10 miles

        SELECT count(*)
        FROM green_taxi
        WHERE trip_distance > 10;

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

