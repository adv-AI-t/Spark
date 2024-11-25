from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
import codecs

spark: SparkSession = SparkSession.builder.appName("MovieRating").getOrCreate()

#here the data is not comma separated, instead, it is   tab     separated
#also, there is no defined schema in the dataset, so we explicitly need to define one

def loadMovieNames():
    movieNames = {}     #creating the name lookup dictionary
    with codecs.open("file:///Users/advait/Documents/Courses/SparkCourse/AdvancedPrograms/Datasets/ml-100k/u.ITEM", "r", encoding="ISO-8859-1", errors="ignore") as file:
        for line in file:
            fields = line.split("|")       #pipe separated 
            movieNames[int(fields[0])] = fields[1]
    return movieNames

nameLookup = spark.sparkContext.broadcast(loadMovieNames())     #broadcasting to each worker in the cluster

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

movies = spark.read.option("sep", "\t").schema(schema=schema).csv("file:///Users/advait/Documents/Courses/SparkCourse/AdvancedPrograms/Datasets/ml-100k/u.data")

movieCounts = movies.groupBy("movieID").count()

#this is a user defined function (udf)
# def lookupName(id):
#     return nameLookup.value[id]

# lookupNameUDF = func.udf(lookupName)

# moviesWithNames = movieCounts.withColumn("Title", lookupNameUDF(func.col("MovieID")))

# sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# sortedMoviesWithNames.show(10)

spark.stop()