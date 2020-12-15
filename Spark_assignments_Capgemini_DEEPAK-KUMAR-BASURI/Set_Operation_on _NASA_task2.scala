// Databricks notebook source
// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120825/2709359236233034/latest.html

val AugustData = sc.textFile("/FileStore/tables/nasa_august.tsv")

val JulyData = sc.textFile("/FileStore/tables/nasa_july.tsv")


// COMMAND ----------

// COMMAND ----------

val AugustRdd = AugustData.map(x => x.split("\t")(0) )

val JulyRdd = JulyData.map(x => x.split("\t")(0) ) 

val intersectionRdd = JulyRdd.intersection(AugustRdd)

intersectionRdd.collect()

// COMMAND ----------


// COMMAND ----------

val FinalRdd = intersectionRdd.filter(x => !(x.contains("host")))
FinalRdd.collect()

// COMMAND ----------

FinalRdd.count()

// COMMAND ----------


