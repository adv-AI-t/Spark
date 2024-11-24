from pyspark.sql import SparkSession, Row

spark:SparkSession = SparkSession.builder.appName("SparkSQL").getOrCreate()

def parseLine(line):
    fields = line.split(",")
    return Row(id = int(fields[0]), name = str(fields[1].encode("utf-8")), age = int(fields[2]), numFriends = int(fields[3]))

lines = spark.sparkContext.textFile("file:///Users/advait/Documents/Courses/SparkCourse/SparkSQL/Datasets/fakefriends.csv")
people = lines.map(parseLine)

peopleSchema = spark.createDataFrame(people).cache()
peopleSchema.createOrReplaceTempView("people")

#using SQL query to get the number of friends per age group, sorted in descending order of age

result = spark.sql("SELECT age, COUNT(*) AS count FROM people GROUP BY age ORDER BY age")

for res in result.collect():
    print(res)

#Now, using function instead of query
#The output is well formatted, just like SQL command-line, unlike the other method, where we just get Row objects

peopleSchema.groupBy("age").count().orderBy("age").show()

spark.stop()