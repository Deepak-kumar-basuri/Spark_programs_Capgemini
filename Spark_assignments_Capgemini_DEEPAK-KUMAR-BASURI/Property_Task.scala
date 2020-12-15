// Databricks notebook source
// DBTITLE 1,Rdd created from property file
// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/4088012569517867/2709359236233034/latest.html

val Propertydata = sc.textFile("/FileStore/tables/Property_data.csv")


// COMMAND ----------

// DBTITLE 1,Removing header
val removeHeader = Propertydata.filter( line => !line.contains("Price"))
removeHeader.take(10)

// COMMAND ----------

// DBTITLE 1,Nested key value pair
val roomRdd = removeHeader.map(x =>  ( x.split(",")(3).toInt , (1, x.split(",")(2).toDouble)  ) )
roomRdd.collect()

// COMMAND ----------

// DBTITLE 1,Reduced key value pair
val ReducedRDD = roomRdd.reduceByKey( (x,y) => ( x._1+y._1, x._2+y._2 ))
ReducedRDD.take(10)

// COMMAND ----------

// DBTITLE 1,Final key value pair
val FinalRdd = ReducedRDD.mapValues( x => x._2/x._1 )
FinalRdd.collect()

// COMMAND ----------

// DBTITLE 1,Prices of correponding bedroom type
for( (bedroom, avg) <- FinalRdd.collect() ) println(bedroom + " : " + avg)

// COMMAND ----------

// DBTITLE 1,Final output saved in the csv format
FinalRdd.saveAsTextFile("PropertyFinal.csv")

// COMMAND ----------


