from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, IntegerType, StringType,
    DoubleType, BooleanType
)

spark = SparkSession.builder.appName("PySparkCheatSheet").getOrCreate()


# ============================================================
# READING CSV

# --- Basic read (no header, all columns become _c0, _c1 ... as strings) ---
df = spark.read.csv("file:///home/takeo/zipcodes.csv")
df.printSchema()
df.show()

# --- With header (uses first row as column names) ---
df2 = spark.read.option("header", True).csv("file:///home/takeo/zipcodes.csv")

# --- With delimiter option ---
df3 = spark.read.options(delimiter=',').csv("file:///home/takeo/zipcodes.csv")

# --- With inferSchema (auto-detects column types, requires 2 passes over data) ---
df4 = spark.read.options(inferSchema='True', delimiter=',') \
    .csv("file:///home/takeo/zipcodes.csv")

# Alternative way to chain options
df4 = spark.read.option("inferSchema", True) \
                .option("delimiter", ",") \
                .csv("file:///home/takeo/zipcodes.csv")

# --- With header + inferSchema + delimiter (most common combo) ---
df5 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("file:///home/takeo/zipcodes.csv")
df5.printSchema()
df5.show()


# ============================================================
# READING CSV WITH CUSTOM SCHEMA
# Use this when you already know the schema ahead of time.
# Avoids the extra pass inferSchema requires.

schema = StructType() \
    .add("RecordNumber",        IntegerType(), True) \
    .add("Zipcode",             IntegerType(), True) \
    .add("ZipCodeType",         StringType(),  True) \
    .add("City",                StringType(),  True) \
    .add("State",               StringType(),  True) \
    .add("LocationType",        StringType(),  True) \
    .add("Lat",                 DoubleType(),  True) \
    .add("Long",                DoubleType(),  True) \
    .add("Xaxis",               IntegerType(), True) \
    .add("Yaxis",               DoubleType(),  True) \
    .add("Zaxis",               DoubleType(),  True) \
    .add("WorldRegion",         StringType(),  True) \
    .add("Country",             StringType(),  True) \
    .add("LocationText",        StringType(),  True) \
    .add("Location",            StringType(),  True) \
    .add("Decommisioned",       BooleanType(), True) \
    .add("TaxReturnsFiled",     StringType(),  True) \
    .add("EstimatedPopulation", IntegerType(), True) \
    .add("TotalWages",          IntegerType(), True) \
    .add("Notes",               StringType(),  True)

# NOTE: Third argument True = nullable allowed, False = NOT nullable

df_with_schema = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load("file:///home/takeo/zipcodes.csv")

df_with_schema.printSchema()
df_with_schema.show()


# ============================================================
#WRITING CSV

# Overwrite mode — replaces existing file if it exists
df_with_schema.write.mode('overwrite').csv("file:///tmp/spark_output/zipcodes")

# Alternative using format()
df_with_schema.write.format("csv").mode('overwrite').save("file:///tmp/spark_output/zipcodes")

# Saving modes:
#   overwrite  — overwrite existing file
#   append     — append to existing file
#   ignore     — ignore if file already exists
#   error      — throw error if file already exists (default)


# ============================================================
#PARQUET
# Sample data
data = [
    ("James ",   "",     "Smith",    "36636", "M", 3000),
    ("Michael ", "Rose", "",         "40288", "M", 4000),
    ("Robert ",  "",     "Williams", "42114", "M", 4000),
    ("Maria ",   "Anne", "Jones",    "39192", "F", 4000),
    ("Jen",      "Mary", "Brown",    "",      "F", -1)
]
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data, columns)

# Write to Parquet
df.write.parquet("file:///tmp/output/people.parquet")

# Read from Parquet
parDF = spark.read.parquet("file:///tmp/output/people.parquet")
parDF.printSchema()
parDF.show()

# Run SQL on Parquet
parDF.createOrReplaceTempView("ParquetTable")
parkSQL = spark.sql("SELECT * FROM ParquetTable WHERE salary >= 4000")
parkSQL.show()


# ============================================================
#ORC
# Write to ORC
parDF.write.orc("file:///tmp/orc/data.orc")

# Read from ORC
df_orc = spark.read.orc("file:///tmp/orc/data.orc")
df_orc.printSchema()
df_orc.show()

# Run SQL on ORC
df_orc.createOrReplaceTempView("ORCTable")
orcSQL = spark.sql("SELECT firstname, dob FROM ORCTable WHERE salary >= 4000")
orcSQL.show()


# ============================================================
#JSON

# Write to JSON
parDF.write.json("file:///tmp/json/data.json")

# Read from JSON
df_json = spark.read.json("file:///tmp/json/data.json")
df_json.printSchema()
df_json.show()


# ============================================================
# QUICK REFERENCE SUMMARY
# ============================================================
#
# READ:
#   spark.read.csv(path)                            — basic, no header, all strings
#   spark.read.option("header", True).csv(path)     — use first row as column names
#   spark.read.options(header=True,
#       inferSchema=True, delimiter=',').csv(path)  — auto-detect types
#   spark.read.format("csv").schema(schema)
#       .option("header", True).load(path)          — custom schema
#   spark.read.parquet(path)                        — read parquet
#   spark.read.orc(path)                            — read orc
#   spark.read.json(path)                           — read json
#
# WRITE:
#   df.write.mode('overwrite').csv(path)
#   df.write.parquet(path)
#   df.write.orc(path)
#   df.write.json(path)
#
# SQL:
#   df.createOrReplaceTempView("tablename")
#   spark.sql("SELECT ... FROM tablename WHERE ...")
#
# SCHEMA TYPES:
#   IntegerType()  DoubleType()  StringType()
#   BooleanType()  DateType()    TimestampType()
# ============================================================