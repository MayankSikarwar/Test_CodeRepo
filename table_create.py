import sys
import os
from pyspark.sql import functions
from error_exec import error_handle
def execute(spark):
    # $example on:generic_load_save_functions$
    try:
         read_recipes =spark.read.option("mode", "PERMISSIVE").json("C://Users//Mayank//Desktop/recipes.json")\
                       .withColumn("date", lit(current_date())) \
                       .withColumn("cookT", expr("substring(cookTime, 3, 2 )"))


         v_recipes = read_recipes.createGlobalTempView("recipes_temp")
         s = spark.sql("CREATE TABLE recipes_final from (SELECT cookTime,datePublished,description \
                    image,ingredients,name,prepTime,recipeYield,url,CASE WHEN cookT BETWEEN 0 AND 30 THEN 'easy' \
                     WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN 'hard' ELSE 'undefined' END AS type from recipes_temp) PARTITIONED BY (date string) \
                     STORED AS orc;")

         """table_recipes = spark.sql("CREATE TABLE recipes_ingestion AS SELECT * from recipes_temp")"""
    except:
        x: int = 2
        error_handle(x)
        exit(0)





