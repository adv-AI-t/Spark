from pyspark.sql import SparkSession, Row

spark:SparkSession = SparkSession.builder.appName("SparkSQL").getOrCreate()

def parseLine(line):
    fields = line.split(",")
    return Row(id = int(fields[0]), name = str(fields[1].encode("utf-8")), age = int(fields[2]), numFriends = int(fields[3]))

lines = spark.sparkContext.textFile("file:///Users/advait/Documents/Courses/SparkCourse/SparkSQL/fakefriends.csv")
people = lines.map(parseLine)

peopleSchema = spark.createDataFrame(people).cache()
peopleSchema.createOrReplaceTempView("people")

teens = spark.sql("SELECT * FROM people WHERE age>=13 AND age<=19")

for teen in teens.collect():
    print(teen)

spark.stop()