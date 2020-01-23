from pyspark.sql import SparkSession


def getSpatk(local : bool = False):

    if local:
        return SparkSession.builder \
            .master("local[*]") \
            .appName("Spark SQL") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .getOrCreate()
