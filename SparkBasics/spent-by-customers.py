from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    cust_id = int(fields[0])
    amount = float(fields[2])
    return(cust_id, amount)


lines = sc.textFile("file:///Users/advait/Documents/Courses/SparkCourse/SparkBasics/Datasets/customer-orders.csv")
parsedLines = lines.map(parseLine)
amountPerCustomer = parsedLines.reduceByKey(lambda x,y : x + y)
results = amountPerCustomer.collect()

for result in results:
    print(result[0], " - ", result[1].__round__(2))