# Spark

Open speak shell
```hql
spark-shell
```

Read and load the data from s3
```hql
val df = spark.read.format("csv").option("header", "true").load("s3://cs5229-flight-delay-eda/data/DelayedFlights-updated.csv")
```

Create table view from the data
```hql
df.createOrReplaceTempView("delay_flights")
```

Validate the table schema
```hql
df.printSchema()
```

Validate the table data
```hql
df.show()
```

## Analysis
Analyse the various delay happens in airlines per year from 2003-2010

### Carrier delay
```hql
spark.time(
    spark.sql(
        "SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AverageCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`")
        .show());
```

Results of 5 iterations
```hql
scala> spark.time(spark.sql("SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AverageCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageCarrierDelay|
+----+-------------------+
|2003| 24.557549755575373|
|2004|  43.64459443230066|
|2005|  28.01977637202288|
|2006| 30.453296261292596|
|2007| 19.850007017971283|
|2008|  28.88346981456985|
|2009|  28.33058554239575|
|2010|  21.89310246015957|
+----+-------------------+

Time taken: 446 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AverageCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageCarrierDelay|
+----+-------------------+
|2003| 24.557549755575373|
|2004|  43.64459443230066|
|2005|  28.01977637202288|
|2006| 30.453296261292596|
|2007| 19.850007017971283|
|2008|  28.88346981456985|
|2009|  28.33058554239575|
|2010|  21.89310246015957|
+----+-------------------+

Time taken: 389 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AverageCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageCarrierDelay|
+----+-------------------+
|2003| 24.557549755575373|
|2004|  43.64459443230066|
|2005|  28.01977637202288|
|2006| 30.453296261292596|
|2007| 19.850007017971283|
|2008|  28.88346981456985|
|2009|  28.33058554239575|
|2010|  21.89310246015957|
+----+-------------------+

Time taken: 381 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AverageCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageCarrierDelay|
+----+-------------------+
|2003| 24.557549755575373|
|2004|  43.64459443230066|
|2005|  28.01977637202288|
|2006| 30.453296261292596|
|2007| 19.850007017971283|
|2008|  28.88346981456985|
|2009|  28.33058554239575|
|2010|  21.89310246015957|
+----+-------------------+

Time taken: 360 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`CarrierDelay` / `ArrDelay`)*100) AS `AverageCarrierDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageCarrierDelay|
+----+-------------------+
|2003| 24.557549755575373|
|2004|  43.64459443230066|
|2005|  28.01977637202288|
|2006| 30.453296261292596|
|2007| 19.850007017971283|
|2008|  28.88346981456985|
|2009|  28.33058554239575|
|2010|  21.89310246015957|
+----+-------------------+

Time taken: 283 ms

```

### NAS delay
```hql
spark.time(
    spark.sql(
        "SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AverageNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`")
        .show());
```

Results of 5 iterations
```hql

scala> spark.time(spark.sql("SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AverageNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------+
|Year|   AverageNASDelay|
+----+------------------+
|2003|29.686276314267346|
|2004| 18.24570061769958|
|2005| 16.63868805373129|
|2006|18.119312329937703|
|2007|30.625925917941924|
|2008| 30.16552562594132|
|2009| 37.63093330628511|
|2010| 33.87351363404217|
+----+------------------+

Time taken: 389 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AverageNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------+
|Year|   AverageNASDelay|
+----+------------------+
|2003|29.686276314267346|
|2004| 18.24570061769958|
|2005| 16.63868805373129|
|2006|18.119312329937703|
|2007|30.625925917941924|
|2008| 30.16552562594132|
|2009| 37.63093330628511|
|2010| 33.87351363404217|
+----+------------------+

Time taken: 242 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AverageNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------+
|Year|   AverageNASDelay|
+----+------------------+
|2003|29.686276314267346|
|2004| 18.24570061769958|
|2005| 16.63868805373129|
|2006|18.119312329937703|
|2007|30.625925917941924|
|2008| 30.16552562594132|
|2009| 37.63093330628511|
|2010| 33.87351363404217|
+----+------------------+

Time taken: 215 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AverageNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------+
|Year|   AverageNASDelay|
+----+------------------+
|2003|29.686276314267346|
|2004| 18.24570061769958|
|2005| 16.63868805373129|
|2006|18.119312329937703|
|2007|30.625925917941924|
|2008| 30.16552562594132|
|2009| 37.63093330628511|
|2010| 33.87351363404217|
+----+------------------+

Time taken: 223 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`NASDelay` / `ArrDelay`)*100) AS `AverageNASDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------+
|Year|   AverageNASDelay|
+----+------------------+
|2003|29.686276314267346|
|2004| 18.24570061769958|
|2005| 16.63868805373129|
|2006|18.119312329937703|
|2007|30.625925917941924|
|2008| 30.16552562594132|
|2009| 37.63093330628511|
|2010| 33.87351363404217|
+----+------------------+

Time taken: 210 ms

```

### Weather delay
```hql
spark.time(
    spark.sql(
        "SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AverageWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`")
        .show());
```

Results of 5 iterations
```hql

scala> spark.time(spark.sql("SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AverageWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageWeatherDelay|
+----+-------------------+
|2003| 7.8319479664511205|
|2004| 6.4475279976916555|
|2005|   5.85069715149616|
|2006|  4.588604183967953|
|2007|  4.042975783210287|
|2008| 3.7254490054008955|
|2009|0.45316615137982363|
|2010| 2.9023312955584664|
+----+-------------------+

Time taken: 221 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AverageWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageWeatherDelay|
+----+-------------------+
|2003| 7.8319479664511205|
|2004| 6.4475279976916555|
|2005|   5.85069715149616|
|2006|  4.588604183967953|
|2007|  4.042975783210287|
|2008| 3.7254490054008955|
|2009|0.45316615137982363|
|2010| 2.9023312955584664|
+----+-------------------+

Time taken: 211 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AverageWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageWeatherDelay|
+----+-------------------+
|2003| 7.8319479664511205|
|2004| 6.4475279976916555|
|2005|   5.85069715149616|
|2006|  4.588604183967953|
|2007|  4.042975783210287|
|2008| 3.7254490054008955|
|2009|0.45316615137982363|
|2010| 2.9023312955584664|
+----+-------------------+

Time taken: 252 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AverageWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageWeatherDelay|
+----+-------------------+
|2003| 7.8319479664511205|
|2004| 6.4475279976916555|
|2005|   5.85069715149616|
|2006|  4.588604183967953|
|2007|  4.042975783210287|
|2008| 3.7254490054008955|
|2009|0.45316615137982363|
|2010| 2.9023312955584664|
+----+-------------------+

Time taken: 202 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`WeatherDelay` / `ArrDelay`)*100) AS `AverageWeatherDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+-------------------+
|Year|AverageWeatherDelay|
+----+-------------------+
|2003| 7.8319479664511205|
|2004| 6.4475279976916555|
|2005|   5.85069715149616|
|2006|  4.588604183967953|
|2007|  4.042975783210287|
|2008| 3.7254490054008955|
|2009|0.45316615137982363|
|2010| 2.9023312955584664|
+----+-------------------+

Time taken: 226 ms

```

### Aircraft delay
```hql
spark.time(
    spark.sql(
        "SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AverageLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`")
        .show());
```

Results of 5 iterations
```hql

scala> spark.time(spark.sql("SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AverageLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------------+
|Year|AverageLateAircraftDelay|
+----+------------------------+
|2003|      37.924225963706164|
|2004|      31.662176952308105|
|2005|      49.490838422749654|
|2006|      46.838787224801735|
|2007|      45.252432744291134|
|2008|       37.22555555408794|
|2009|      33.585314999939314|
|2010|      41.331052610239794|
+----+------------------------+

Time taken: 271 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AverageLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------------+
|Year|AverageLateAircraftDelay|
+----+------------------------+
|2003|      37.924225963706164|
|2004|      31.662176952308105|
|2005|      49.490838422749654|
|2006|      46.838787224801735|
|2007|      45.252432744291134|
|2008|       37.22555555408794|
|2009|      33.585314999939314|
|2010|      41.331052610239794|
+----+------------------------+

Time taken: 192 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AverageLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------------+
|Year|AverageLateAircraftDelay|
+----+------------------------+
|2003|      37.924225963706164|
|2004|      31.662176952308105|
|2005|      49.490838422749654|
|2006|      46.838787224801735|
|2007|      45.252432744291134|
|2008|       37.22555555408794|
|2009|      33.585314999939314|
|2010|      41.331052610239794|
+----+------------------------+

Time taken: 227 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AverageLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------------+
|Year|AverageLateAircraftDelay|
+----+------------------------+
|2003|      37.924225963706164|
|2004|      31.662176952308105|
|2005|      49.490838422749654|
|2006|      46.838787224801735|
|2007|      45.252432744291134|
|2008|       37.22555555408794|
|2009|      33.585314999939314|
|2010|      41.331052610239794|
+----+------------------------+

Time taken: 207 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `AverageLateAircraftDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+------------------------+
|Year|AverageLateAircraftDelay|
+----+------------------------+
|2003|      37.924225963706164|
|2004|      31.662176952308105|
|2005|      49.490838422749654|
|2006|      46.838787224801735|
|2007|      45.252432744291134|
|2008|       37.22555555408794|
|2009|      33.585314999939314|
|2010|      41.331052610239794|
+----+------------------------+

Time taken: 199 ms

```

### Security delay
```hql
spark.time(
    spark.sql(
        "SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AverageSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`")
        .show());
```

Results of 5 iterations
```hql

scala> spark.time(spark.sql("SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AverageSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+--------------------+
|Year|AverageSecurityDelay|
+----+--------------------+
|2003|                 0.0|
|2004|                 0.0|
|2005|                 0.0|
|2006|                 0.0|
|2007| 0.22865853658536586|
|2008|                 0.0|
|2009|                 0.0|
|2010|                 0.0|
+----+--------------------+

Time taken: 209 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AverageSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+--------------------+
|Year|AverageSecurityDelay|
+----+--------------------+
|2003|                 0.0|
|2004|                 0.0|
|2005|                 0.0|
|2006|                 0.0|
|2007| 0.22865853658536586|
|2008|                 0.0|
|2009|                 0.0|
|2010|                 0.0|
+----+--------------------+

Time taken: 270 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AverageSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+--------------------+
|Year|AverageSecurityDelay|
+----+--------------------+
|2003|                 0.0|
|2004|                 0.0|
|2005|                 0.0|
|2006|                 0.0|
|2007| 0.22865853658536586|
|2008|                 0.0|
|2009|                 0.0|
|2010|                 0.0|
+----+--------------------+

Time taken: 200 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AverageSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+--------------------+
|Year|AverageSecurityDelay|
+----+--------------------+
|2003|                 0.0|
|2004|                 0.0|
|2005|                 0.0|
|2006|                 0.0|
|2007| 0.22865853658536586|
|2008|                 0.0|
|2009|                 0.0|
|2010|                 0.0|
+----+--------------------+

Time taken: 202 ms

scala> spark.time(spark.sql("SELECT `Year`, avg((`SecurityDelay` / `ArrDelay`)*100) AS `AverageSecurityDelay` FROM delay_flights GROUP BY `Year` ORDER BY `Year`").show());
+----+--------------------+
|Year|AverageSecurityDelay|
+----+--------------------+
|2003|                 0.0|
|2004|                 0.0|
|2005|                 0.0|
|2006|                 0.0|
|2007| 0.22865853658536586|
|2008|                 0.0|
|2009|                 0.0|
|2010|                 0.0|
+----+--------------------+

Time taken: 218 ms

```
