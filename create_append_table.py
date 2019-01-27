from error_exec import error_handle
from pyspark.sql import functions


def execute(spark):
    if spark.sql("SHOW TABLES LIKE" "recipes_ingestion ").collect().length == 0:
        try:
            create_table = spark.sql("CREATE TABLE recipes_ingestion from (SELECT cookTime,datePublished,description \
                        image,ingredients,name,prepTime,recipeYield,url,CASE WHEN cookT BETWEEN 0 AND 30 THEN 'easy' \
                         WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN 'hard' ELSE 'undefined' END AS type from recipes_temp) PARTITIONED BY (date string) \
                         STORED AS orc;")

        except:
            x= 2
            error_handle(x)
            exit(0)
    else:
         try:
              drop_query = "ALTER TABLE recipes_ingestion DROP IF EXISTS PARTITION (DATE='$DATE')"
              drop = spark.sql(drop_query)
              append_table = spark.sql("SELECT cookTime, \
                              datePublished,description,image,ingredients,name,prepTime,recipeYield,url,date,CASE WHEN cookT BETWEEN 0 \
                              AND 30 THEN 'easy' WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN \
                              'hard' ELSE 'undefined' END AS type from recipes_temp")

              append_table.write.partitionBy("DATE") \
                              .saveAsTable("recipes_ingestion", 'format = "orc"', 'mode = "append"', 'path = "<path to orc>" ')

         except:
                x: int = 3
                error_handle(x)
                exit(0)