from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrdersSorted")
sc = SparkContext(conf = conf)

def parseLines(line):
    fields = line.split(",")
    return(int(fields[0]), float(fields[2]))

lines = sc.textFile("file:///Users/advait/Documents/Courses/SparkCourse/SparkBasics/Datasets/customer-orders.csv")
parsedLines = lines.map(parseLines)
customerAmountMapping = parsedLines.reduceByKey(lambda x, y: x + y)
flipped = customerAmountMapping.map(lambda pair : (pair[1], pair[0]))
sorted = flipped.sortByKey()
results = sorted.collect()

for result in results:
    print(result[1], " : ", result[0].__round__(2))
