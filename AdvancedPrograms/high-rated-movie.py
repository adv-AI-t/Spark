from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

spark: SparkSession = SparkSession.builder.appName("MovieRating").create()

#here the data is not comma separated, instead, it is   tab     separated
#also, there is no defined schema in the dataset, so we explicitly need to define one

