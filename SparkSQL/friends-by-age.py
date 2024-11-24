from pyspark.sql import SparkSession, functions

spark: SparkSession = SparkSession.builder.appName("ByAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/advait/Documents/Courses/SparkCourse/SparkSQL/Datasets/fakefriends-header.csv")

ageAndFriends = people.select("age", "friends")

ageAndFriends.groupBy("age").agg(functions.round(functions.avg("friends"), 2).alias("avg_no_friends")).sort("age").show()

spark.stop()
