# PYSpark - Which is faster - RDD or Data Frame
Python and PYSpark referene codes

Simple PYSpark code to load multuple data compressed data files from local file system and process them to answer
business questions:

Two approaches:
1. The old RDD way 
2. The new Data Frame way

Definitely the Data Frames are faster than RDD in PYSpark.

Data set used:
10 Days of R Packages downloaded log files (65.7 MB size) from http://cran-logs.rstudio.com. 
You can download all the logs files since 2013
which is approximately 10 GB.

Business question:
What is the most popular R Pacakges downloaded?

The RDD API took 2.2 minutes whereas the Data Frame API took 48 seconds only. 
The system used was laptop with 4 core and 6 GB RAM.

Screen shot of Spark UI for RDD API:
![alt text](https://raw.githubusercontent.com/shivamms/pysparkcodes/master/RPackages-RDD.png)

Screen shot of Spark UI for Data Frame API:
![alt text](https://raw.githubusercontent.com/shivamms/pysparkcodes/master/RPackages-DF.png)

