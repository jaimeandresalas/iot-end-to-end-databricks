# Databricks notebook source
# MAGIC %md
# MAGIC # Iot Ingest, Processing and Analytics on Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Secrets in Databricks

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("Event Hub Name","iothub-ehub-iot-jasm-25244516-2eb9d8dd3c")
dbutils.widgets.text("iot_cs","Endpoint=sb://ihsuprodblres013dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=uWGe01LyHT2sUHYKXQdKNx6OyIiIpzmqpAIoTEMRvts=;EntityPath=iothub-ehub-iot-jasm-25244516-2eb9d8dd3c")
dbutils.widgets.text("synapse_container", "iot")
dbutils.widgets.text("synapse_group_container", "synapsejasm")

# COMMAND ----------

storage_account = dbutils.secrets.get("iot","storage_account")
storage_key = dbutils.secrets.get("iot","storage_key")
container_name = dbutils.secrets.get("iot","container_name")
iot_cs = dbutils.secrets.get("iot","iot_cs")
synapse_key = dbutils.secrets.get("iot","synapse_key")
s_container_name = dbutils.widgets.get("synapse_container")
s_group_name = dbutils.widgets.get("synapse_group_container")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount ADLS to Databricks without app registration

# COMMAND ----------

url = "wasbs://" + container_name + "@" + storage_account + ".blob.core.windows.net/"
config = "fs.azure.account.key." + storage_account + ".blob.core.windows.net"
url_synapse = "wasbs://" + s_container_name + "@" + s_group_name + ".blob.core.windows.net/"
mount_folder = "/mnt/iot"
mounted_list = dbutils.fs.mounts()

mounted_exist = False
for item in mounted_list:
    if mount_folder in item[0]:
        mounted_exist = True
        break

if not mounted_exist:
    dbutils.fs.mount(source = url, mount_point = mount_folder, extra_configs = {config : storage_key})

# COMMAND ----------

# Setup storage locations for all data
ROOT_PATH = mount_folder
BRONZE_PATH = ROOT_PATH + "/bronze/"
SILVER_PATH = ROOT_PATH + "/silver/"
GOLD_PATH = ROOT_PATH + "/gold/"
SYNAPSE_PATH = url_synapse + "/synapse/"
CHECKPOINT_PATH = ROOT_PATH + "/checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurate Spark for Streaming Process

# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# Other initializations
IOT_CS = dbutils.widgets.get("iot_cs")
ehConf = { 
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(IOT_CS),
  'ehName':dbutils.widgets.get("Event Hub Name")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Process Code

# COMMAND ----------

# Pyspark and ML Imports
import os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

# Schema of incoming data from IoT hub
schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"

# Read directly from IoT Hub using the EventHubs library for Databricks
iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
)

# Split our IoT Hub stream into separate streams and write them both into their own Delta locations
write_turbine_to_delta = (
  #iot_stream.filter('temperature is null')                                           # Filter out turbine telemetry from other data streams
  iot_stream.filter(iot_stream.temperature.isNull())
    .select('date','timestamp','deviceId','rpm','angle')                             # Extract the fields of interest
    .writeStream.format('delta')                                                     # Write our stream to the Delta format
    .option("mergeSchema", "true")
    #.partitionBy('date')                                                             # Partition our data by Date for performance
    .option("checkpointLocation", CHECKPOINT_PATH + "turbine_raw")                   # Checkpoint so we can restart streams gracefully
    .start(BRONZE_PATH + "turbine_raw")                                              # Stream the data into an ADLS Path
)

write_weather_to_delta = (
  iot_stream.filter(iot_stream.temperature.isNotNull())                              # Filter out weather telemetry only
    .select('date','deviceid','timestamp','temperature','humidity','windspeed','winddirection') 
    .writeStream.format('delta')                                                     # Write our stream to the Delta format
    .option("mergeSchema", "true")
    #.partitionBy('date')                                                             # Partition our data by Date for performance
    .option("checkpointLocation", CHECKPOINT_PATH + "weather_raw")                   # Checkpoint so we can restart streams gracefully
    .start(BRONZE_PATH + "weather_raw")                                              # Stream the data into an ADLS Path
)

# Create the external tables once data starts to stream in
while True:
  try:
    spark.sql(f'CREATE TABLE IF NOT EXISTS turbine_raw USING DELTA LOCATION "{BRONZE_PATH + "turbine_raw"}"')
    spark.sql(f'CREATE TABLE IF NOT EXISTS weather_raw USING DELTA LOCATION "{BRONZE_PATH + "weather_raw"}"')
    break
  except:
    pass

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- We can query the data directly from storage immediately as soon as it starts streams into Delta 
# MAGIC SELECT * FROM turbine_raw WHERE deviceid = 'WindTurbine-1'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Bronze to Delta Silver

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# Create functions to merge turbine and weather data into their target Delta tables
def merge_delta(incremental, target): 
  incremental.dropDuplicates(['date','window','deviceid']).createOrReplaceTempView("incremental")
  
  try:
    # MERGE records into the target table using the specified join key
    incremental._jdf.sparkSession().sql(f"""
      MERGE INTO delta.`{target}` t
      USING incremental i
      ON i.date=t.date AND i.window = t.window AND i.deviceId = t.deviceid
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  except:
    # If the †arget table does not exist, create one
    incremental.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(target)
    #incremental.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("date").save(target)
    
turbine_b_to_s = (
  spark.readStream.format('delta').table("turbine_raw")                        # Read data as a stream from our source Delta table
    .groupBy('deviceId','date',F.window('timestamp','5 minutes'))              # Aggregate readings to hourly intervals
    .agg(F.avg('rpm').alias('rpm'), F.avg("angle").alias("angle"))
    .writeStream                                                               # Write the resulting stream
    .option("mergeSchema", "true")
    .foreachBatch(lambda i, b: merge_delta(i, SILVER_PATH + "turbine_agg"))    # Pass each micro-batch to a function
    #.outputMode("complete")
    .outputMode("update")                                                      # Merge works with update mode
    .option("checkpointLocation", CHECKPOINT_PATH + "turbine_agg")             # Checkpoint so we can restart streams gracefully
    .start()
)


weather_b_to_s = (
  spark.readStream.format('delta').table("weather_raw")                        # Read data as a stream from our source Delta table
    .groupBy('deviceid','date',F.window('timestamp','5 minutes'))              # Aggregate readings to hourly intervals
    .agg({"temperature":"avg","humidity":"avg","windspeed":"avg","winddirection":"last"})
    .selectExpr('date','window','deviceid','`avg(temperature)` as temperature','`avg(humidity)` as humidity',
                '`avg(windspeed)` as windspeed','`last(winddirection)` as winddirection')
    .writeStream                                                               # Write the resulting stream
    .option("mergeSchema", "true")
    .foreachBatch(lambda i, b: merge_delta(i, SILVER_PATH + "weather_agg"))    # Pass each micro-batch to a function
    .outputMode("update")                                                      # Merge works with update mode
    .option("checkpointLocation", CHECKPOINT_PATH + "weather_agg")             # Checkpoint so we can restart streams gracefully
    .start()
)

# Create the external tables once data starts to stream in
while True:
  try:
    spark.sql(f'CREATE TABLE IF NOT EXISTS turbine_agg USING DELTA LOCATION "{SILVER_PATH + "turbine_agg"}"')
    spark.sql(f'CREATE TABLE IF NOT EXISTS weather_agg USING DELTA LOCATION "{SILVER_PATH + "weather_agg"}"')
    break
  except:
    pass

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As data gets merged in real-time to our hourly table, we can query it immediately
# MAGIC SELECT * 
# MAGIC FROM turbine_agg as t 
# MAGIC JOIN weather_agg as w 
# MAGIC ON (t.date=w.date and t.window = w.window) 
# MAGIC WHERE t.deviceid='WindTurbine-1' 
# MAGIC ORDER BY t.window DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Silver to Delta Gold

# COMMAND ----------

# Read streams from Delta Silver tables and join them together on common columns (date & window)
turbine_agg = spark.readStream.format('delta').option("ignoreChanges", True).table('turbine_agg')
weather_agg = spark.readStream.format('delta').option("ignoreChanges", True).table('weather_agg').drop('deviceid')
turbine_enriched = turbine_agg.join(weather_agg, ['date','window'])

# Write the stream to a foreachBatch function which performs the MERGE as before
merge_gold_stream = (
  turbine_enriched
    .selectExpr('date','deviceid','window.start as window','rpm','angle','temperature','humidity','windspeed','winddirection')
    .writeStream 
    .foreachBatch(lambda i, b: merge_delta(i, GOLD_PATH + "turbine_enriched"))
    .option("checkpointLocation", CHECKPOINT_PATH + "turbine_enriched")         
    .start()
)

# Create the external tables once data starts to stream in
while True:
  try:
    spark.sql(f'CREATE TABLE IF NOT EXISTS turbine_enriched USING DELTA LOCATION "{GOLD_PATH + "turbine_enriched"}"')
    break
  except:
    pass

# COMMAND ----------


