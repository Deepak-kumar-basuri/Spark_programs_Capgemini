// Databricks notebook source
// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120840/2709359236233034/latest.html

val NumberData = sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

def PrimeNo(i:Int): Boolean = {
  if (i <= 1)
        false
    else if (i == 2)
        true
    else
        !(2 until i).exists(n => i % n == 0)
}

// COMMAND ----------

val header = NumberData.first()

val data1 = NumberData.filter(row => row != header)

val data2 = data1.map(x => x.toInt)

val data3 = data2.filter(x => PrimeNo(x))

data3.take(3)

// COMMAND ----------


val productRDD = data3.reduce( (x,y) => x+y )

// COMMAND ----------


