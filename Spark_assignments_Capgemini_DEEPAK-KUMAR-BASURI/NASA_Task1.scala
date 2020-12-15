// Databricks notebook source
// DBTITLE 1,RDD created for both files
// File uploaded to /FileStore/tables/nasa_july.tsv
//File uploaded to /FileStore/tables/nasa_august.tsv

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/4465796836760466/2709359236233034/latest.html

val augustRdd = sc.textFile("/FileStore/tables/nasa_august.tsv")

val julyRdd = sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

// DBTITLE 1,union between august and july NASA file
val unionRdd = augustRdd.union(julyRdd)
unionRdd.take(2)

// COMMAND ----------

// DBTITLE 1,header of union file
val header = unionRdd.first


// COMMAND ----------

// DBTITLE 1,first approach to remove header
unionRdd.filter(line =>line != header).take(2)

// COMMAND ----------

// DBTITLE 1,second approach to remove header
// Second approach
def headerRemover(line:String): Boolean = !(line.startsWith("host"))

// COMMAND ----------

// DBTITLE 1,removing header
val finalRdd = unionRdd.filter(x => headerRemover(x))
finalRdd.take(2)

// COMMAND ----------

// DBTITLE 1,Datas where responce was zero and bytes size > 1000
val zeroResponse = finalRdd.filter(x => x.split("\t")(5).toInt == 0)
val Bytes = finalRdd.filter(x => x.split("\t")(6).toInt >= 1000)
(zeroResponse.collect() , Bytes.collect())


// COMMAND ----------

// DBTITLE 1,total datas whose bytes > 1000
Bytes.count()

// COMMAND ----------

// DBTITLE 1,total datas whose bytes > 1000 or reponse is 0
val nasadata = finalRdd.filter(x => ( x.split("\t")(5).toInt == 0 || x.split("\t")(6).toInt >= 1000))

nasadata.count()

// COMMAND ----------

// DBTITLE 1,taking sample data (i.e. 20%) out of the whole data set
//Sample on our data
finalRdd.sample(withReplacement = true, fraction=0.20).collect()

// COMMAND ----------


