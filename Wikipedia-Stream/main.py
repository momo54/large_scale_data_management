# start the spark session

from pyspark.streaming import StreamingContext
from sseclient import SSEClient
import threading

import uuid
import json


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window 
from pyspark.sql.functions import col, from_unixtime, window


def test():
    msgs = SSEClient('https://stream.wikimedia.org/v2/stream/recentchange')

    i=0
    with open("test.json", "w") as outfile:
        for e in msgs:
            print(e)
            outfile.write(f"{e}\n")
            i=i+1
            if i>10:
                break

def read_wikipedia():
    msgs = SSEClient('https://stream.wikimedia.org/v2/stream/recentchange')

    buffer=[]
    i=0
    for e in msgs:
      buffer.append(e)
      if len(buffer)>100:
        with open(f"streaming/{str(uuid.uuid4())}.json", "w") as outfile:
          for m in buffer:
            outfile.write(f"{m}\n")
          outfile.close()
          #json.dump([ob.__dict__ for ob in buffer], outfile)
        buffer.clear()
        i=i+1
        #if i>3:
        #  return


def get_schema(spark):
    #don't forget the r for raw string...
    data=r"""{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q114775139","request_id":"31f7d9b1-12c4-4de4-bee7-fbdd8c055c4f","id":"6580fd51-0bde-4f48-8186-8b1f202c4e14","dt":"2022-10-20T13:20:31Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":4248495942},"id":1809138679,"type":"edit","namespace":0,"title":"Q114775139","comment":"/* wbsetclaim-create:2||1 */ [[Property:P6634]]: ellen-rudnick-364a793","timestamp":1666272031,"user":"Korimako","bot":false,"minor":false,"patrolled":true,"length":{"old":2211,"new":2575},"revision":{"old":1754930719,"new":1754930819},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"‎<span dir=\"auto\"><span class=\"autocomment\">Created claim: </span></span> <a href=\"/wiki/Property:P6634\" title=\"Property:P6634\">Property:P6634</a>: ellen-rudnick-364a793"}"""

    #don't forget to build a list ie [data]...
    rdd=spark.sparkContext.parallelize([data])
    df=spark.read.json(rdd)
    df.show()

    df.printSchema()

    #schema is computed ;)
    return df.schema

def run_query_user_count(spark):
    lines = spark \
        .readStream \
        .option("cleanSource","delete") \
        .format("json") \
        .schema(get_schema(spark)) \
        .load("streaming") \

    results=lines.groupBy("user").count()

    query = results \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

def process_batch(df, epoch_id):
    print(f"Processing batch {epoch_id}")
    df.select("timestamp").show(n=5,truncate=False)


def run_query_user_timestamp(spark):
    lines = spark \
        .readStream \
        .option("cleanSource","delete") \
        .format("json") \
        .schema(get_schema(spark)) \
        .load("streaming") 
        
    # Convert BIGINT to TIMESTAMP
    lines_with_timestamp = lines.withColumn("timestamp", from_unixtime(col("timestamp") ).cast("timestamp"))
    lines_wk=lines_with_timestamp.withWatermark("timestamp", "10 seconds") \
    

# Set up the streaming query
    query = lines_wk.writeStream \
        .foreachBatch(process_batch) \
        .start()
    query.awaitTermination()



## tumbling window
def run_query_user_count_tumbling(spark):
    lines = spark \
        .readStream \
        .option("cleanSource","delete") \
        .format("json") \
        .schema(get_schema(spark)) \
        .load("streaming") 
        
    # Convert BIGINT to TIMESTAMP
    lines_with_timestamp = lines.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    lines_wk=lines_with_timestamp.withWatermark("timestamp", "10 seconds") \

    results=lines_wk.groupBy(
        window("timestamp", "10 seconds"),
        "user") \
        .count()

    query = results \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

def main():
    spark = SparkSession \
    .builder \
    .appName("Wikipedia Event") \
    .getOrCreate()

    threading.Thread(target=read_wikipedia).start()
    #run_query_user_count(spark)
#    run_query_user_timestamp(spark)

    run_query_user_count_tumbling(spark)
    spark.stop()
main()