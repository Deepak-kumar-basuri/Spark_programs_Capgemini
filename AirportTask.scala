// Databricks notebook source
// DBTITLE 1,Uploaded airport.csv file
// /FileStore/tables/airports.text
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/895101600532720/2709359236233034/latest.html

val apdata = sc.textFile("/FileStore/tables/airports.text")


// COMMAND ----------

// DBTITLE 1,countries whose latitude greater than 40 or countries name Iceland
val CountryIceland = apdata.filter(line => line.split(",")(6) >= "40" || line.split(",")(3) == "\"Iceland\"" )

CountryIceland.take(10)

// COMMAND ----------

// DBTITLE 1,saving the file in .csv format
CountryIceland.saveAsTextFile("CountryIceland.csv")

// COMMAND ----------

// DBTITLE 1,the number of occurance of timestamp where altitude is even
val evenAlt = apdata.filter(line => (line.split(",")(8).toInt % 2 == 0 ))

val tsData = evenAlt.map(x => x.split(",")(11))

tsData.take(10)

// COMMAND ----------

evenAlt.countByValue()

// COMMAND ----------


