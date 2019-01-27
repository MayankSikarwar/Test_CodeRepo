import argparse
from pyspark.sql import SparkSession
import table_create
import beef_view
import append_table


pipelines = {"table_create": table_create, "beef_view": beef_view, "append_table": append_table}


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
