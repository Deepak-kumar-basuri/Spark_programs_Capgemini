// Databricks notebook source
// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120832/2709359236233034/latest.html

val Values = List("Modi", "Narendra", "Raghuram", "Rajan", "Obama", "Putin")

// COMMAND ----------

val datardd = sc.parallelize(Values)

// COMMAND ----------

datardd.count()

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val value = List(1,2,3,4,5)

val valuerdd = sc.parallelize(value)

// COMMAND ----------

val productRDD = valuerdd.reduce( (x,y) => x*y )

productRDD

// COMMAND ----------


