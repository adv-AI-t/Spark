from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/advait/Documents/Courses/SparkCourse/AdvancedPrograms/Datasets/Marvel+Names")

lines = spark.read.text("file:///Users/advait/Documents/Courses/SparkCourse/AdvancedPrograms/Datasets/Marvel+Graph")

#there are several lines which have space separated ids. The first id is the id we are considering,
#and the number of ids in the line except the first one are the number of connections

#so the first value will go into the "id" column, and the total number of values in the line -1 will be the number of connections,
#that will go into the "connections" column

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
              .withColumn("connection", func.size(func.split(func.col("value"), " "))-1)\
              .groupBy("id").agg(func.sum("connection").alias("connection"))

mostPopular = connections.sort(func.col("connection").asc()).first()

mostPopularHeroName = names.filter(func.col("id") == mostPopular[0]).select("name").show()

spark.stop()