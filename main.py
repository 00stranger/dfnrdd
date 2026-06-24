from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import Row
from pyspark.sql.functions import col, lit
from pyspark.sql.types import ArrayType
from covidAnalysis import covid_data_analysis

def ecomm(spark, sdata):

    structSchema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id",    IntegerType(), True),
        StructField("product_id",     IntegerType(), True),
        StructField("product_name",   StringType(),  True),
        StructField("category",       StringType(),  True),
        StructField("price",          DoubleType(),  True),
        StructField("quantity",       IntegerType(), True)
    ])

    typed_rdd = sdata.map(lambda f: Row(
        transaction_id=int(f[0]),
        customer_id=int(f[1]),
        product_id=int(f[2]),
        product_name=f[3],
        category=f[4],
        price=float(f[5]),
        quantity=int(f[6])
    ))

    transactions_df = spark.createDataFrame(typed_rdd, schema=structSchema)
    transactions_df.printSchema()
    transactions_df.show()
    return transactions_df

def filterFunc(obtainedDF):
    rdd = obtainedDF.rdd
    high_quantity_rdd = rdd.filter(lambda x: int(x[6])>1)
    print(high_quantity_rdd.collect())
    return high_quantity_rdd

def divPurch(obtainedDF):
    rdd1 = obtainedDF.rdd
    products_flat_rdd = rdd1.flatMap(lambda x: [x[3]])
    print(products_flat_rdd.collect())

#pair rdd practice
#def redux(obtainedDF):
    #theSchema = StructType([
        #StructField("customer_id", IntegerType(), True),
        #StructType([
            #StructField("product_name", StringType(), True),

        #])
    #])
#filter, withColumn
def rddOps(obtainedDF):
    df_new = obtainedDF
    df_new.filter(df_new.category=="Electronics").show(truncate=False)
    df_new.withColumn("points_obtained",lit(500)).show()  #lit = literal function to fill columns with constant values
    df_new.withColumnRenamed("price", "price(in $)").show(truncate=False)

#Nested columns
def nestCols(dataCol):

    thisSchema = StructType([
        StructField('trophies_won', StructType([
            StructField('UCL', IntegerType(), True),
            StructField('Premier', IntegerType(), True),
            StructField("Europa", IntegerType(), True)
        ])),
        StructField('players_famous', ArrayType(StringType()), True),
        StructField('clubs_name', StringType(), True),
        StructField('position_this_season', IntegerType(), True)
    ])
    thisDF = spark.createDataFrame(data=dataCol, schema=thisSchema)
    thisDF.printSchema()
    thisDF.show()

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("ninja").getOrCreate()

    sdata = [
        "1,101,5001,Laptop,Electronics,1000.0,1",
        "2,102,5002,Headphones,Electronics,50.0,2",
        "3,101,5003,Book,Books,20.0,3",
        "4,103,5004,Laptop,Electronics,1000.0,1",
        "5,102,5005,Chair,Furniture,150.0,1"
    ]

    sdataP = spark.sparkContext.parallelize(sdata)
    sdata_split = sdataP.map(lambda x: x.split(","))
    obtainedDF = ecomm(spark, sdata_split)
    filterFunc(obtainedDF)
    divPurch(obtainedDF)
    #redux(obtainedDF)
    rddOps(obtainedDF)

    #new data for nested columns
    dataCol = [
    ((3,5,8),["Madueke", "Saliba", "Martinelli"],"Arsenal",1),
    ((11,18,1),["James","Palmer","Nkunku"],"Chelsea",3),
    ((3,8,12),["Gea","Martinez","Fernandez"],"Manu",4),
    ((6,11,9),["DeBruyne","Doku"],"Mancity",7),
    ((4,15,7),["Kane","Son"],"Spurs",8),
    ((5,14,19),["Gerrard","Salah"],"Liverpool",10)
 ]
    nestCols(dataCol)

    path_to_covidcase= "file:///home/takeon/pycharmprojects/CovidCases.csv"
    covid_data_analysis(spark,path_to_covidcase)
