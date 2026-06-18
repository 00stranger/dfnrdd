from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,
                                   StructField, StringType, IntegerType)

def wordCount(spark, filepath):
    kdd = spark.sparkContext.textFile(filepath)
    # print(kdd.collect())
    kdd1 = kdd.flatMap(lambda x: (x.split(" ")))
    # print(kdd1.collect())
    kdd2 = kdd1.map(lambda x: (x, 1))
    # print(kdd2.collect())
    kdd3 = kdd2.reduceByKey(lambda x, y: x + y)
    print(kdd3.collect())
    #kdd4 = kdd3.map(lambda x: (x[1], x[0])).sortByKey()
    # print(kdd4.collect())
    #kdd5 = kdd4.filter(lambda x: 'Nep' in x[1])
    # print(kdd5.collect())
    #print(kdd3.collect())
    #first = kdd3.reduce(lambda a, b: (a[0] + b[0], a[1]))
    #print(first)

def dfFunc(spark):
    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    rdd = spark.sparkContext.parallelize(data)
    dfFromRDD1 = rdd.toDF()
    dfFromRDD1.printSchema()

    dfFromRDD1 = rdd.toDF(columns)
    dfFromRDD1.printSchema()
    dfFromRDD1.show()

def StSf(spark):
    data = [("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
            ]
    schema = StructType([StructField("firstname", StringType(), True), StructField("middlename",
                         StringType(), True), StructField("lastname", StringType(), True),
                         StructField("id", StringType(), True),StructField("gender", StringType(),
                                True), StructField("salary", IntegerType(), True)])
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show()

def nestStruct(spark):
    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
    ])
    df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
    df2.printSchema()
    df2.show(truncate=False)


if __name__ == "__main__":
    spark= SparkSession.builder.master("local[2]").appName("pyprac").getOrCreate()
    wordCount(spark, "file:///home/pycharmProjects/cotton.txt")
    wordCount(spark, "file:///home/pycharmProjects/text.txt")
    dfFunc(spark)
    StSf(spark)
    nestStruct(spark)