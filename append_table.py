from error_exec import error_handle
def final_app(spark):
    try:
        read_recipes = spark.read.option("mode", "PERMISSIVE").json("C://Users//Mayank//Desktop/recipes.json") \
            .withColumn("date", lit(current_date())) \
            .withColumn("cookT", expr("substring(cookTime, 3, 2 )"))
        print("read or timestamp add failed append fail")
        drop_query = "ALTER TABLE recipes_ingestio DROP IF EXISTS PARTITION (DATE='$DATE')".format("val=target_partition")
        drop = spark.sql(drop_query)
        appe= spark.sql("SELECT cookTime, \
             datePublished,description,image,ingredients,name,prepTime,recipeYield,url,date,CASE WHEN cookT BETWEEN 0 \
             AND 30 THEN 'easy' WHEN cookT BETWEEN 30 AND 45 THEN 'medium' WHEN cookT BETWEEN 45 AND 60 THEN 'hard' ELSE 'undefined' END AS type from recipes_temp") \
             .write.partitionBy("partition_col") \
             .saveAsTable("recipes_ingestion", 'format = "orc"', 'mode = "append"', 'path = <path to orc>"')
    except:
        x = 3
        error_handle(x)
        exit(0)
