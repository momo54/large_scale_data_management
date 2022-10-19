from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

schema = StructType([
    StructField("person_ID",IntegerType(),True), 
    StructField("last", StringType(), True),
    StructField("middle", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("title", StringType(), True)])

people_df = spark.readStream.format("csv").\
   schema(schema). \
   load("streaming")
   # option("header", True). \

print(people_df.isStreaming)

## seems to work but nothing in console...
#results_df = people_df.select("*")
#query = results_df.writeStream \
#  .format("console") \
#  .outputMode("append") \
#  .start()

#query.awaitTermination()


query = people_df.select("*").groupby("last").count().writeStream \
  .format("console") \
  .outputMode("complete") \
  .start()

query.awaitTermination()
