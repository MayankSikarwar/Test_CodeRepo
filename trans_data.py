from error_exec import error_handle
from pyspark.sql import functions


def execute(spark):
    try:
       read_recipes = spark.read.option("mode", "PERMISSIVE").json("C://Users//Mayank//Desktop/recipes.json") \
        .withColumn("date", lit(current_date())) \
        .withColumn("cookT", expr("substring(cookTime, 3, 2 )"))
       v_recipes = read_recipes.createGlobalTempView("recipes_temp")
    except:
       x: int = 1
       error_handle(x)
       exit(0)