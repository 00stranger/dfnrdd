from pyspark.sql import SparkSession
if __name__ == '__main__':
   # print()


    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    #print(rdd.count())
    rdd = spark.sparkContext.textFile("file:///home/pycharmProjects/text.txt")
    rdd2 = rdd.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))
    rdd5 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd5.collect())

