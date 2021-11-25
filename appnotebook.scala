// Databricks notebook source
// Now we need to display our data properly
import org.apache.spark.sql.types._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._


val connectionString = "Endpoint=sb://eventhubnamespace-sali.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=utPDfRo9MlwN6fImA81YFPGzUtFBCo1/uQtFbUjC2pw=;EntityPath=dbhub"
val eventHubsConf = EventHubsConf(connectionString)
.setStartingPosition(EventPosition.fromStartOfStream)
// Reference for the different options - https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs


// get_json_object - Extracts json object from a json string
// Here we want to get the body part of the messages that are sent from Azure Event Hub
// The body of the messages have to be first converted to string
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)  
  .load()
  .select(get_json_object(($"body").cast("string"), "$.records").alias("records"))

// Next we want to extract all of the json objects. Each metric will be a seperate JSON object
val maxMetrics = 30 
val jsonElements = (0 until maxMetrics)
                     .map(i => get_json_object($"records", s"$$[$i]"))

val newDF = eventhubs
  .withColumn("records", explode(array(jsonElements: _*))) // Here _* is a special expression in spark to get each element of the array
  .where(!isnull($"records")) 

// from_json - Parses a column containing a JSON string into a MapType with StringType as keys type, StructType or ArrayType with the specified schema
// Now we need to convert each json string to a json object with a defined schema

val dataSchema = new StructType()
        .add("count", IntegerType)
        .add("total", IntegerType)
        .add("minimum", IntegerType)
        .add("maximum", IntegerType)
        .add("resourceId", StringType)
        .add("time", StringType)
        .add("metricName", StringType)
        .add("timeGrain", StringType)
        .add("average", IntegerType)

val df=newDF.withColumn("records",from_json(col("records"),dataSchema))

// Next we need to ensure there are multiple columns for each property of the JSON object
val finalDF=df.select(col("records.*"))
display(finalDF)
