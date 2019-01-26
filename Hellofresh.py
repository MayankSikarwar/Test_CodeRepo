from pyspark.sql import SparkSession
from pyspark.sql import Row
import doctest
from pyspark.sql import Row, SparkSession
import pyspark.sql.group
import sys
import os
if __name__ == "__main__"(argv):
    spark = SparkSession \
        .builder \
        .appName("Hellofresh_test") \
        .getOrCreate()
    print('test')

    for arg in argv:
        if sys.argv[1]:
            basic__example(spark)
            """table invalidate and refreh Impala"""
            os.system('beeline -isolation=TRANSACTION_READ_UNCOMMITED -u "$Connect" -u $user -p $PASSWORD -e "USE default; invalidate medatadat recipes_ingestion;refresh recipes_ingestion ;" ')
        elif sys.argv[2]:
            final_app(spark)
            basic_trns(spark)
            """table invalidate and refreh Impala"""
            os.system(
                'beeline -isolation=TRANSACTION_READ_UNCOMMITED -u "$Connect" -u $user -p $PASSWORD -e "USE default;refresh recipes_ingestion ;" ')
    spark.stop()


def basic__example(spark):
    # $example on:generic_load_save_functions$
      try:
         read_recipes =spark.read.option("mode", "PERMISSIVE").json("C://Users//Mayank//Desktop/recipes.json")\
                       .withColumn("date", lit(current_date())) \
                       .withColumn("cookT", expr("substring(cookTime, 3, 2 )"))
      except:
         print("read or timestamp add failed fail")


v_recipes = read_recipes.createGlobalTempView("recipes_temp")
s = ss.sql("CREATE TABLE recipes_final from (SELECT cookTime,datePublished,description \
           image,ingredients,name,prepTime,recipeYield,url,CASE WHEN cookT BETWEEN 0 AND 30 THEN 'easy' \
            WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN 'hard' ELSE 'undefined' END AS type from recipes_temp) PARTITIONED BY (date string) \
            STORED AS orc;")  \
    .persist()
table_recipes = spark.sql("CREATE TABLE recipes_ingestion AS SELECT * from recipes_temp")


def basic_trns(spark):
    try:
        table_recipes = spark.sql("create view beef_dishes_view as(select * from people where ingredients like '%beef%)")
    except:
        print("Beef view failed")


def final_app(spark):
    try:
        read_recipes = spark.read.option("mode", "PERMISSIVE").json("C://Users//Mayank//Desktop/recipes.json") \
            .withColumn("date", lit(current_date())) \
            .withColumn("cookT", expr("substring(cookTime, 3, 2 )"))
    except:
        print("read or timestamp add failed append fail")
drop_query = "ALTER TABLE recipes_ingestio DROP IF EXISTS PARTITION (DATE='$DATE')".format("val=target_partition")

appe= spark.sql("SELECT cookTime, \
             datePublished,description,image,ingredients,name,prepTime,recipeYield,url,date,CASE WHEN cookT BETWEEN 0 \
             AND 30 THEN 'easy' WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN 'hard' ELSE 'undefined' END AS type from recipes_temp") \
    .write.partitionBy("partition_col").saveAsTable("recipes_ingestion", 'format = "orc"', 'mode = "append"', 'path = <path to parquet>"')