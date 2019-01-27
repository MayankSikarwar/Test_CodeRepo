from error_exec import error_handle


def execute(spark):
    try:
        table_recipes = spark.sql("create view beef_dishes_view as(select * from people where ingredients like '%beef%)")
    except:
        x: int = 4
        error_handle(x)
        exit(0)