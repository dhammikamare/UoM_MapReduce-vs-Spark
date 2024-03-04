# UoM_MapReduce-vs-Spark

The airlines market has been faced losses due to the flight delay and there are many reasons for delaying a flight. In this Analysis, we  analyse the various delay happens in airlines per year and run the queries as follows.

Year wise carrier delay from 2003-2010
Year wise NAS delay from 2003-2010
Year wise Weather delay from 2003-2010
Year wise late aircraft delay from 2003-2010
Year wise security delay from 2003-2010

Data description is available at https://www.kaggle.com/code/adveros/flight-delay-eda-exploratory-data-analysis/notebook

We are using Hadoop for storing and processing a huge amount of data, for storing its uses HDFS (Hadoop Distributed File System) and for processing its uses MapReduce. For analysing we use hive which uses hiveQL statement which runs over MapReduce framework to analyse the data. And we also analyse the data by using spark which uses Spark-SQL and runs over spark framework. To compare MapReduce and Apache Spark, we use AWS EMR. 