from pyspark.sql import SparkSession, Row

spark: SparkSession = SparkSession.builder.appName("Dataframes").getOrCreate()

#this time, we have a csv file that has a header (the topmost row has names of what the data in that column stands for, eg name, age, etc)

people = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/advait/Documents/Courses/SparkCourse/SparkSQL/Datasets/fakefriends-header.csv")

print("Inferred Schema:")
people.printSchema()

print("Table")
people.select("*").show()

print("Adults")
people.filter(people.age > 18).show()

print("Group by age")
people.groupBy("age").count().show()

spark.stop()