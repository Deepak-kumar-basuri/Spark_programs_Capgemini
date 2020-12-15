// Databricks notebook source
// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120832/2709359236233034/latest.html

val airports = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val dataRdd = data.map(line => ( line.split(",")(1), line.split(",")(3) ))
dataRdd.take(3)

// COMMAND ----------

// DBTITLE 1,All those airport which are not in canada
val NonCanada = dataRdd.filter( x => x._2 != "\"Canada\"")
NonCanada.take(2)

// COMMAND ----------

NonCanada.filter(x => x._2 == "\"Canada\"").take(2)


// COMMAND ----------

// DBTITLE 1,Creating a key value pair
val listData = List("johnson 1998","obama 2001", "Putin 2100", "modi 1997")


// COMMAND ----------

val kvrdd = sc.parallelize(listData)
val newrdd = kvrdd.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))
rddnew.take(3)

// COMMAND ----------

newrdd.mapValues(x => x+10).take(3)

// COMMAND ----------

val value = data.map(line => ( line.split(",")(1), line.split(",")(11) ))
value.take(5)

// COMMAND ----------

value.mapValues(x => x.toLowerCase).take(5)

// COMMAND ----------


