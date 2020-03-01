import unittest
import os
from pyspark.sql import SparkSession

from src import data_analysis


class TestMain(unittest.TestCase):
    def test_main(self):
        SparkSession.builder \
            .master("local[*]") \
            .appName("Spark SQL") \
            .getOrCreate()

        path = os.path.dirname(__file__)
        path_arr = os.path.split(path)
        print(path_arr)
        data_analysis.analyse(path_arr[0] + "/data/survey_results_public.csv")


if __name__ == '__main__':
    unittest.main()
