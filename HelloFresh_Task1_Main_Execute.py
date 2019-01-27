import argparse
from pyspark.sql import SparkSession
import trans_data
import beef_view
import create_append_table

pipelines = {"trans_data": trans_data, "create_append_table": create_append_table, "beef_view": beef_view}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline')
    args = parser.parse_args()

    spark = spark = SparkSession \
                 .builder \
                 .appName("Hello_fresh_test") \
                 .getOrCreate()
    pipelines[args.pipeline].execute(spark)

    spark.stop()
