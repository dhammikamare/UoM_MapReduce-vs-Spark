# MapReduce

Open hive
```hql
hive
```

Create Hadoop data table on s3
```hql
CREATE TABLE delay_flights (
    Id INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay DOUBLE,
    DepDelay DOUBLE,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted DOUBLE,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://cs5229-flight-delay-eda/hive/';
```

Validate the table schema
```hql
DESCRIBE delay_flights;
```

Read and load the data from s3 to Hadoop table
```hql
LOAD DATA INPATH 's3://cs5229-flight-delay-eda/data/DelayedFlights-updated.csv' OVERWRITE INTO TABLE delay_flights;
```


## Analysis
Analyse the various delay happens in airlines per year from 2003-2010

### Carrier delay
```hql
SELECT Year, avg((CarrierDelay/ArrDelay)*100) as AverageCarrierDelay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
```

Results of 5 iterations
```hql
hive> SELECT Year, avg((CarrierDelay/ArrDelay)*100) as AverageCarrierDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063844_eb0be875-b54e-45d6-9f42-58110b5d56d7
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 6.92 s
----------------------------------------------------------------------------------------------
OK
2010    21.89310246015957
2009    28.33058554239575
2008    28.88346981456985
2007    19.850007017971283
2006    30.453296261292596
2005    28.01977637202288
2004    43.64459443230066
2003    24.557549755575373
NULL    NULL
Time taken: 7.223 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((CarrierDelay/ArrDelay)*100) as AverageCarrierDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063853_6064584d-8dc9-4dfc-8d87-1825287095e5
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.05 s
----------------------------------------------------------------------------------------------
OK
2010    21.89310246015957
2009    28.33058554239575
2008    28.88346981456985
2007    19.850007017971283
2006    30.453296261292596
2005    28.01977637202288
2004    43.64459443230066
2003    24.557549755575373
NULL    NULL
Time taken: 1.295 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((CarrierDelay/ArrDelay)*100) as AverageCarrierDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063857_5081afc8-2173-45dd-9704-e9a0493d889e
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.32 s
----------------------------------------------------------------------------------------------
OK
2010    21.89310246015957
2009    28.33058554239575
2008    28.88346981456985
2007    19.850007017971283
2006    30.453296261292596
2005    28.01977637202288
2004    43.64459443230066
2003    24.557549755575373
NULL    NULL
Time taken: 1.534 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((CarrierDelay/ArrDelay)*100) as AverageCarrierDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063900_6ec1b412-3aac-41d4-b847-0a7e87033d79
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.04 s
----------------------------------------------------------------------------------------------
OK
2010    21.89310246015957
2009    28.33058554239575
2008    28.88346981456985
2007    19.850007017971283
2006    30.453296261292596
2005    28.01977637202288
2004    43.64459443230066
2003    24.557549755575373
NULL    NULL
Time taken: 1.256 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((CarrierDelay/ArrDelay)*100) as AverageCarrierDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063903_ff9edf0d-cea5-4d60-8b01-cc416dbad28f
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 0.93 s
----------------------------------------------------------------------------------------------
OK
2010    21.89310246015957
2009    28.33058554239575
2008    28.88346981456985
2007    19.850007017971283
2006    30.453296261292596
2005    28.01977637202288
2004    43.64459443230066
2003    24.557549755575373
NULL    NULL
Time taken: 1.122 seconds, Fetched: 9 row(s)

```


### NAS delay
```hql
SELECT Year, avg((NASDelay/ArrDelay)*100) as AverageNASDelay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
```

Results of 5 iterations
```hql
hive> SELECT Year, avg((NASDelay/ArrDelay)*100) as AverageNASDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063914_df1c5cb5-4cdb-477f-afd4-21fc3ff98a4f
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 6.45 s
----------------------------------------------------------------------------------------------
OK
2010    33.87351363404217
2009    37.63093330628511
2008    30.16552562594132
2007    30.625925917941924
2006    18.119312329937703
2005    16.63868805373129
2004    18.24570061769958
2003    29.686276314267346
NULL    NULL
Time taken: 6.716 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((NASDelay/ArrDelay)*100) as AverageNASDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063922_47cf5814-b467-4244-a191-da770f97745f
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 0.95 s
----------------------------------------------------------------------------------------------
OK
2010    33.87351363404217
2009    37.63093330628511
2008    30.16552562594132
2007    30.625925917941924
2006    18.119312329937703
2005    16.63868805373129
2004    18.24570061769958
2003    29.686276314267346
NULL    NULL
Time taken: 1.157 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((NASDelay/ArrDelay)*100) as AverageNASDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063926_341d80f7-88e1-415d-a86d-aab0878cd582
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.36 s
----------------------------------------------------------------------------------------------
OK
2010    33.87351363404217
2009    37.63093330628511
2008    30.16552562594132
2007    30.625925917941924
2006    18.119312329937703
2005    16.63868805373129
2004    18.24570061769958
2003    29.686276314267346
NULL    NULL
Time taken: 1.514 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((NASDelay/ArrDelay)*100) as AverageNASDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063929_43b52e1a-6d06-4e09-86c7-ae68e986d4c8
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.07 s
----------------------------------------------------------------------------------------------
OK
2010    33.87351363404217
2009    37.63093330628511
2008    30.16552562594132
2007    30.625925917941924
2006    18.119312329937703
2005    16.63868805373129
2004    18.24570061769958
2003    29.686276314267346
NULL    NULL
Time taken: 1.255 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((NASDelay/ArrDelay)*100) as AverageNASDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063932_c6aee99a-acdd-456d-bce3-36c991261ed9
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.30 s
----------------------------------------------------------------------------------------------
OK
2010    33.87351363404217
2009    37.63093330628511
2008    30.16552562594132
2007    30.625925917941924
2006    18.119312329937703
2005    16.63868805373129
2004    18.24570061769958
2003    29.686276314267346
NULL    NULL
Time taken: 1.466 seconds, Fetched: 9 row(s)

```

### Weather delay
```hql
SELECT Year, avg((WeatherDelay/ArrDelay)*100) as AverageWeatherDelay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
```

Results of 5 iterations
```hql
hive> SELECT Year, avg((WeatherDelay/ArrDelay)*100) as AverageWeatherDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304063957_939f5404-e52d-4fd6-8714-9876c832b6af
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 6.64 s
----------------------------------------------------------------------------------------------
OK
2010    2.9023312955584664
2009    0.45316615137982363
2008    3.7254490054008955
2007    4.042975783210287
2006    4.588604183967953
2005    5.85069715149616
2004    6.4475279976916555
2003    7.8319479664511205
NULL    NULL
Time taken: 6.806 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((WeatherDelay/ArrDelay)*100) as AverageWeatherDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064006_52a0b5af-c2a6-46fb-8bfc-32f5235ec8b9
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.26 s
----------------------------------------------------------------------------------------------
OK
2010    2.9023312955584664
2009    0.45316615137982363
2008    3.7254490054008955
2007    4.042975783210287
2006    4.588604183967953
2005    5.85069715149616
2004    6.4475279976916555
2003    7.8319479664511205
NULL    NULL
Time taken: 1.463 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((WeatherDelay/ArrDelay)*100) as AverageWeatherDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064010_6f2f4837-b0e7-48c6-801b-fc02cabc5576
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.12 s
----------------------------------------------------------------------------------------------
OK
2010    2.9023312955584664
2009    0.45316615137982363
2008    3.7254490054008955
2007    4.042975783210287
2006    4.588604183967953
2005    5.85069715149616
2004    6.4475279976916555
2003    7.8319479664511205
NULL    NULL
Time taken: 1.292 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((WeatherDelay/ArrDelay)*100) as AverageWeatherDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064014_e8001dce-7a4f-450d-b49a-d907bb8eb5b2
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.14 s
----------------------------------------------------------------------------------------------
OK
2010    2.9023312955584664
2009    0.45316615137982363
2008    3.7254490054008955
2007    4.042975783210287
2006    4.588604183967953
2005    5.85069715149616
2004    6.4475279976916555
2003    7.8319479664511205
NULL    NULL
Time taken: 1.293 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((WeatherDelay/ArrDelay)*100) as AverageWeatherDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064018_43ac6d80-aee8-4e6b-9b72-0960bb358876
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.10 s
----------------------------------------------------------------------------------------------
OK
2010    2.9023312955584664
2009    0.45316615137982363
2008    3.7254490054008955
2007    4.042975783210287
2006    4.588604183967953
2005    5.85069715149616
2004    6.4475279976916555
2003    7.8319479664511205
NULL    NULL
Time taken: 1.259 seconds, Fetched: 9 row(s)

```

### Aircraft delay
```hql
SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as AverageLateAircraftDelay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
```

Results of 5 iterations
```hql
hive> SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as AverageLateAircraftDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064038_42879938-10af-46d4-a33e-1c7d02b8d8c5
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 6.91 s
----------------------------------------------------------------------------------------------
OK
2010    41.331052610239794
2009    33.585314999939314
2008    37.22555555408794
2007    45.252432744291134
2006    46.838787224801735
2005    49.490838422749654
2004    31.662176952308105
2003    37.924225963706164
NULL    NULL
Time taken: 7.082 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as AverageLateAircraftDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064047_cf139aee-0775-4d99-af66-b313808da5c0
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.28 s
----------------------------------------------------------------------------------------------
OK
2010    41.331052610239794
2009    33.585314999939314
2008    37.22555555408794
2007    45.252432744291134
2006    46.838787224801735
2005    49.490838422749654
2004    31.662176952308105
2003    37.924225963706164
NULL    NULL
Time taken: 1.428 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as AverageLateAircraftDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064050_802e0511-6e73-4d66-aff7-ac7526502225
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.00 s
----------------------------------------------------------------------------------------------
OK
2010    41.331052610239794
2009    33.585314999939314
2008    37.22555555408794
2007    45.252432744291134
2006    46.838787224801735
2005    49.490838422749654
2004    31.662176952308105
2003    37.924225963706164
NULL    NULL
Time taken: 1.17 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as AverageLateAircraftDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064052_0cf0d558-9620-4058-bcc9-a867a4f8df17
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.11 s
----------------------------------------------------------------------------------------------
OK
2010    41.331052610239794
2009    33.585314999939314
2008    37.22555555408794
2007    45.252432744291134
2006    46.838787224801735
2005    49.490838422749654
2004    31.662176952308105
2003    37.924225963706164
NULL    NULL
Time taken: 1.274 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as AverageLateAircraftDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064055_f2d0b10e-ea78-4c15-989f-6f1964f2be96
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.13 s
----------------------------------------------------------------------------------------------
OK
2010    41.331052610239794
2009    33.585314999939314
2008    37.22555555408794
2007    45.252432744291134
2006    46.838787224801735
2005    49.490838422749654
2004    31.662176952308105
2003    37.924225963706164
NULL    NULL
Time taken: 1.348 seconds, Fetched: 9 row(s)

```

### Security delay
```hql
SELECT Year, avg((SecurityDelay/ArrDelay)*100) as AverageSecurityDelay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
```

Results of 5 iterations
```hql
hive> SELECT Year, avg((SecurityDelay/ArrDelay)*100) as AverageSecurityDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064107_cc913928-6831-44ca-89fe-52c5fc49a70e
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 6.37 s
----------------------------------------------------------------------------------------------
OK
2010    0.0
2009    0.0
2008    0.0
2007    0.22865853658536586
2006    0.0
2005    0.0
2004    0.0
2003    0.0
NULL    NULL
Time taken: 6.529 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((SecurityDelay/ArrDelay)*100) as AverageSecurityDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064121_7d483553-eed7-4f45-9121-07b3bec304b7
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 6.21 s
----------------------------------------------------------------------------------------------
OK
2010    0.0
2009    0.0
2008    0.0
2007    0.22865853658536586
2006    0.0
2005    0.0
2004    0.0
2003    0.0
NULL    NULL
Time taken: 6.359 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((SecurityDelay/ArrDelay)*100) as AverageSecurityDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064129_a07b1f59-a1df-47dd-a19f-abd75aef1de9
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.32 s
----------------------------------------------------------------------------------------------
OK
2010    0.0
2009    0.0
2008    0.0
2007    0.22865853658536586
2006    0.0
2005    0.0
2004    0.0
2003    0.0
NULL    NULL
Time taken: 1.457 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((SecurityDelay/ArrDelay)*100) as AverageSecurityDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064133_3b7c913f-39e3-4504-ba13-00519dbd6710
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.34 s
----------------------------------------------------------------------------------------------
OK
2010    0.0
2009    0.0
2008    0.0
2007    0.22865853658536586
2006    0.0
2005    0.0
2004    0.0
2003    0.0
NULL    NULL
Time taken: 1.488 seconds, Fetched: 9 row(s)
hive> SELECT Year, avg((SecurityDelay/ArrDelay)*100) as AverageSecurityDelay from delay_flights GROUP BY Year ORDER BY Year DESC;
Query ID = hadoop_20240304064135_0250d5cb-e926-448d-8bab-32dd0e034ee7
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1709528996866_0005)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 1.20 s
----------------------------------------------------------------------------------------------
OK
2010    0.0
2009    0.0
2008    0.0
2007    0.22865853658536586
2006    0.0
2005    0.0
2004    0.0
2003    0.0
NULL    NULL
Time taken: 1.348 seconds, Fetched: 9 row(s)

```
