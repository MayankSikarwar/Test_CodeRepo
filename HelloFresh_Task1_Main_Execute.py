import argparse
from pyspark.sql import SparkSession , HiveContext
import trans_data
import beef_view
import create_append_table

pipelines = {"trans_data": trans_data, "create_append_table": create_append_table, "beef_view": beef_view}
"""Executor for all the scripts for application"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline')
    args = parser.parse_args()

    """Spark session is passed  to other scripts"""
    spark = SparkSession.builder.appName("Hello_fresh_test").config('hive', 'location').getOrCreate().enablehivesuppot()

    pipelines[args.pipeline].execute(spark)

    spark.stop()
