from pyspark.sql import functions as F
from pyspark.sql.functions import column, sum, max
from pyspark.sql.window import Window
def covid_data_analysis(spark,path_to_covidcase):
    covidDF = spark.read.csv(path_to_covidcase, header=True, inferSchema=True)
    #now changing the name of column 'infection case' to infection_source
    covid_renameDF= covidDF.withColumnRenamed("infection_case", "infection_source")
    #display only selected columns from our data
    covidDF_smallview = covid_renameDF.select(["province", "city", "infection_source", "confirmed"])
    covidDF_smallview.show()

    #changing the datatype of column confirmed to Integer by typecasting
    covid_renameDF= covid_renameDF.withColumn("confirmed", column("confirmed").cast("integer"))
    #displaying TotalConfirmed and max confirmed from province
    covid_confirmedDF = covid_renameDF.groupBy("province", "city")\
                        .agg(
                            sum("confirmed").alias("TotalConfirmed"),
                            max("confirmed").alias("MaxFromOneConfirmedCase")
                            )
    covid_confirmedDF.show()

    #now we display the province and city based on 2 highest total no. of confirmed covid cases.
    windowSpec = Window.orderBy(F.desc("TotalConfirmed"))
    covid_renameDF.groupBy("province", "city")\
                  .agg(F.sum("confirmed").alias("TotalConfirmed"))\
                   .withColumn("case_rank", F.dense_rank().over(windowSpec))\
                    .filter(F.column("case_rank") <= 2)\
                    .show()

    #time to display only covid cases specific to Daegu province with more than 10 total confirmed covid cases
    covid_renameDF.groupBy("province", "city")\
                  .agg(F.sum("confirmed").alias("TotalConfirmed"))\
                  .filter((F.column("province")=="Daegu") & (F.column("TotalConfirmed") > 10))\
                  .show()

    #let's practice dropping some columns now
    drop_list = [" case_id", "latitude", "longitude"]    #case_id has a space because that's how it's named in excel file
    df_dropped = covid_renameDF.drop(*drop_list)
    df_dropped.show()