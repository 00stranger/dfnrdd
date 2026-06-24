
from pyspark.sql.functions import column, sum, max

def covid_data_analysis(spark,path_to_covidcase):
    covidDF = spark.read.csv(path_to_covidcase, header=True, inferSchema=True)
    covid_renameDF= covidDF.withColumnRenamed("infection_case", "infection_source")
    #covid_renameDF.show()
    covid_renameDF.createOrReplaceTempView("covidtable")
    covidSql = spark.sql("select province, city, infection_source, confirmed from covidtable")
    #covidSql.show()
    covid_renameDF= covid_renameDF.withColumn("confirmed", column("confirmed").cast("integer"))
    covid_renameDF.printSchema()

    covidSql1 = spark.sql('select province, city, sum(confirmed) as TotalConfirmed,\
                          max(confirmed) as MaxFromOneConfirmedCase\
                          From covidtable Group By province, city\
                          Order by TotalConfirmed ASC')
    #covidSql1.show(156, truncate=False)
    covidSql1.show()
    rankSql = spark.sql('select * from (select province, city,sum(confirmed) as TotalConfirmed,\
                        Dense_RANK() over (order by sum(confirmed) desc) as case_rank\
                        From covidtable\
                        Group By province,city)\
                        Where case_rank <= 2')
    rankSql.show()

    daeguSql = spark.sql('Select * from (select province, city, sum(confirmed) as TotalConfirmed\
                         From covidtable\
                         Group BY province,city)\
                         where province="Daegu" And TotalConfirmed > 10')
    daeguSql.show()
    #let's practice dropping some columns now
    drop_list = [" case_id", "latitude", "longitude"]    #case_id has a space because that's how it's named in excel file 
    df_dropped = covid_renameDF.drop(*drop_list)
    df_dropped.show()