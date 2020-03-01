import os
import sys
from pyspark import StorageLevel
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def get_current_path():
    return os.path.dirname(__file__)


# Get spark session
spark = SparkSession.builder.getOrCreate()


# Arguments : Data path
def analyse(path):

    # read file
    df = spark.read.option('header', True) \
        .option('inferSchema', True) \
        .csv(path) \
        .persist(StorageLevel(True, True, False, False))

    df.createOrReplaceTempView("survey_results")

    # functions
    get_developer_os()
    get_developer_type()
    get_contrib_open_source(df)
    get_coding_as_hobby(df)


# Get developer type
def get_developer_type():
    total = spark.sql('select count(*) total from survey_results where DevType <> "NA"').collect()[0]["total"]
    spark.sql("select explode(split(DevType, ';')) as DevType from survey_results where DevType <> 'NA'") \
        .groupBy("DevType") \
        .agg(f.round(f.count('DevType') * 100 / total, 1).alias("Percentage")) \
        .orderBy('Percentage', ascending=False) \
        .show(20, False)


# Get developer OS
def get_developer_os():
    spark.sql('''
    with t as (select count(*) as total from survey_results where opSys <> 'NA')
    select s.opSys, round((count(s.opSys)*100)/(select t.total from t),1) count
     from survey_results s 
     where opSys <> 'NA'
     group by opSys 
     order by opSys desc''').show()


def calculate_percentage(df: DataFrame, col_name: str):
    return df.groupBy(col_name).agg(f.round(f.count(col_name) * 100 / df.count(), 1).alias('Percentage')) \
        .orderBy('Percentage', ascending=False)


def get_contrib_open_source(df: DataFrame):
    df.groupBy('OpenSourcer').agg(f.round(f.count('OpenSourcer') * 100 / df.count(), 1).alias('Percentage')) \
        .orderBy('Percentage', ascending=False) \
        .show(20, False)


def get_coding_as_hobby(df: DataFrame):
    calculate_percentage(df, 'Hobbyist').show(20, False)


# Arguments : Data path
def main():

    if len(sys.argv) != 2:
        raise Exception("Invalid arguments passed")

    path = sys.argv[1]
    print(path)

    analyse(path)

if __name__ == "__main__":
    main()
