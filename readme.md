1. Download Spark from  http://spark.apache.org/downloads.html

2. extract file using 
    * gzip -d <filename.tgz>
    * tar xvf <filename.tar>
    
3. Set environment variables
    * setx SPARK_HOME C:\opt\spark\spark-2.4.4-bin-hadoop2.7
    * setx PYSPARK_DRIVER_PYTHON python
    
4. Connect pyspark to pycharm IDE
    * File -> Settings -> Project Structure -> add Content Root 
        C:\Spark\spark-2.3.1-bin-hadoop2.7\python
