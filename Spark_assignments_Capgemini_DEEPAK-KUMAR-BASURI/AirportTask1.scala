// Databricks notebook source
// DBTITLE 1,RDD created for the airport.csv file
// /FileStore/tables/airports.text
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/895101600532720/2709359236233034/latest.html

val Airportdata = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

// DBTITLE 1,countries whose latitude greater than 40 or countries name Iceland
val Country_Iceland = Airportdata.filter(line => line.split(",")(6) >= "40" || line.split(",")(3) == "\"Iceland\"" )

Country_Iceland.take(10)

// COMMAND ----------

// DBTITLE 1,saving the file in .csv format
Country_Iceland.saveAsTextFile("Country_Iceland.csv")

// COMMAND ----------

// DBTITLE 1,the number of occurance of timestamp where altitude is even
val EvenAltitude = apdata.filter(line => (line.split(",")(8).toInt % 2 == 0 ))

val TimestampData = evenAlt.map(x => x.split(",")(11))

TimestampData.take(10)

// COMMAND ----------

// DBTITLE 1,Total altitude which are even number
EvenAltitude.countByValue()

// COMMAND ----------


