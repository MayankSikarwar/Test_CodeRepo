from error_exec import error_handle
from pyspark.sql import functions
import os


def execute(spark):
    if spark.sql("SHOW TABLES LIKE" "recipes_ingestion ").collect().length == 0:
        try:
            """create table if not exist"""

            create_table = spark.sql("CREATE TABLE recipes_ingestion from (SELECT cookTime,datePublished,description \
                        image,ingredients,name,prepTime,recipeYield,url,CASE WHEN cookT BETWEEN 0 AND 30 THEN 'easy' \
                         WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN 'hard' ELSE 'undefined' END AS type from recipes_temp) PARTITIONED BY (date string) \
                         STORED AS orc;")

            """invalidate and refresh table in IMPALA"""
            os.system(
                'beeline -isolation=TRANSACTION_READ_UNCOMMITED \
                -u "$Connect" -u $user -p $PASSWORD -e "USE default; invalidate medatadat recipes_ingestion;refresh recipes_ingestion ;" ')

        except:
            x= 2
            error_handle(x)
            exit(0)
    else:
         try:
              drop_query = "ALTER TABLE recipes_ingestion DROP IF EXISTS PARTITION (DATE='$DATE')"
              drop = spark.sql(drop_query)
              """Append table if exist"""

              append_table = spark.sql("SELECT cookTime, \
                              datePublished,description,image,ingredients,name,prepTime,recipeYield,url,date,CASE WHEN cookT BETWEEN 0 \
                              AND 30 THEN 'easy' WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN \
                              'hard' ELSE 'undefined' END AS type from recipes_temp")

              append_table.write.partitionBy("DATE").saveAsTable("recipes_ingestion", 'format = "orc"', 'mode = "append"', 'path = "<path to orc>" ')


              """refrsh table in impala"""

              os.system(
                  'beeline -isolation=TRANSACTION_READ_UNCOMMITED \
                   -u "$Connect" -u $user -p $PASSWORD -e "USE default;refresh recipes_ingestion ;" ')

         except:
                x: int = 3
                error_handle(x)
                exit(0)