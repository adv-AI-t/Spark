from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

spark: SparkSession = SparkSession.builder.appName("MovieRating").getOrCreate()

#here the data is not comma separated, instead, it is   tab     separated
#also, there is no defined schema in the dataset, so we explicitly need to define one

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

movies = spark.read.option("sep", "\t").schema(schema=schema).csv("file:///Users/advait/Documents/Courses/SparkCourse/AdvancedPrograms/Datasets/ml-100k/u.data")

topMovieIDs = movies.groupBy("movieID").count().orderBy(func.desc("count"))

topMovieIDs.show(10)    #top 10 most popular movies

spark.stop()