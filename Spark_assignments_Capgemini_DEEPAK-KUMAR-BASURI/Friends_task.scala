// Databricks notebook source
// DBTITLE 1,RDD created for the friends file
// Databricks notebook source
// /FileStore/tables/FriendsData.csv
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/4088012569517876/2709359236233034/latest.html
val data = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

data.take(10)

// COMMAND ----------

// DBTITLE 1,Removing the header
val removeHead = data.filter(x => !(x.contains("name")) )
removeHead.take(10)

// COMMAND ----------

val friendRdd = remHead.map( x => ( x.split(",")(2).toInt , (1, x.split(",")(3).toInt) ) )
friendRdd.take(10)

// COMMAND ----------

// DBTITLE 1,reducing by key
val sum = friendRdd.reduceByKey( (x,y) => (x._1+y._1, x._2+y._2) )

sum.collect()

// COMMAND ----------

// DBTITLE 1,Average of the reduce pairs
val avg = sum.mapValues(x => x._2/x._1)

avg.collect() 

// COMMAND ----------

// DBTITLE 1,Task-1 Average Number Of Friends 

for( (age,friends) <- avg.collect())  println(age+" ----> "+friends)

// COMMAND ----------

// DBTITLE 1,Task-2 Maximum number of friends for each age

val NewRdd = removeHead.map( x => ( x.split(",")(2).toInt ,  x.split(",")(3).toInt ) )
NewRdd.take(10)


// COMMAND ----------

val maxVal = NewRdd.reduceByKey(math.max(_,_))
maxVal.collect()

// COMMAND ----------

// DBTITLE 1,age and corresponding maximum number of friends
for( (age,maxFrnd) <- maxVal.collect())  println(age+" ----> "+maxFrnd)

// COMMAND ----------


