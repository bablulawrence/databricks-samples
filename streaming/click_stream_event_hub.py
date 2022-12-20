# Databricks notebook source
# MAGIC %sh
# MAGIC #--Download clickstream file from Wikipedia--
# MAGIC wget "https://dumps.wikimedia.org/other/clickstream/2022-11/clickstream-enwiki-2022-11.tsv.gz" -P "/tmp/wikipedia/"

# COMMAND ----------

#--Save the data to delta table---
from pyspark.sql.functions import *
from pyspark.sql.types import *

inputFilePath = "/mnt/data/public/wikipedia/clickstream-dewiki-2022-11.tsv.gz"
bronzeTablePath = "/mnt/data/bronze/wikipedia/clickstream_eng_2022_11"

dbutils.fs.cp("file:/tmp/wikipedia/clickstream-enwiki-2022-11.tsv.gz", inputFilePath)

schema = StructType([
    StructField("prev", StringType(), True),
    StructField("curr", StringType(), True),
    StructField("type", StringType(), True),
    StructField("n", IntegerType(), True)
])

df = spark.read.csv(sep="\t",schema=schema,path=inputFilePath).limit(100)
df.write.format("delta").save(bronzeTablePath)


# COMMAND ----------

#--Generate streaming data from delta table and publish to Event Hub--

#<provide Event Hub namespace name>
EH_NS_NAME = "az-cslabs-event-hub-ns"
#<provide Event Hub shared access key>
SAKEY = "9Pd1a6FG2IGYdry/bk7CLQWkihoxhZkTVDVds5EiTpk="
#<provide topic name>
TOPIC = "public.clickstream_wikipedia_bronze"

CONN_STRING = f"Endpoint=sb://{EH_NS_NAME}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={SAKEY}"
LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
EH_SASL = f'{LOGIN_MODULE} required username="$ConnectionString" password="{CONN_STRING}";'

checkpointLocation = f"{bronzeTablePath}/checkpoint"

#Read data from delta table as a stream
df = ( spark.readStream
          .format("delta")
          .load(bronzeTablePath)
     )

#Create key and value from dataframe columns for publishing to Event Hub
df = ( df.withColumn("key", col("curr"))
       .withColumn("value", to_json(struct("prev","curr", "type", "n"))))

df.display()

#Write stream to Event Hub
( df.writeStream 
  .format("kafka")
  .option("kafka.bootstrap.servers", f"{EH_NS_NAME}.servicebus.windows.net:9093")
  .option("topic", TOPIC)
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", EH_SASL)
  .option("kafka.request.timeout.ms", "60000")
  .option("kafka.session.timeout.ms", "60000")
  .option("checkpointLocation", checkpointLocation)
  .start()
)

# COMMAND ----------

#--Read the click stream from Event Hub---

df = ( spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",  f"{EH_NS_NAME}.servicebus.windows.net:9093")
    .option("subscribe", TOPIC)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
)

#Covert key and value from base64 to string
df = (df.withColumn("key", col("key").cast("string"))
        .withColumn("value",col("value").cast("string"))         
     )

df.display()

# COMMAND ----------

#--Covert value column from Json to columns using schema
df = ( df.select(from_json(col("value"), schema).alias("row"))
         .select("row.*")     
     )

df.display()
