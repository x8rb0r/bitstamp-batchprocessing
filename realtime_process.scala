// Databricks notebook source
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
  
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

//'price','amount','type'
case class DeviceData(price: Double, amount: Double, transactiontype: Int)

// COMMAND ----------


val ssc = new StreamingContext(sc, Seconds(30))

// COMMAND ----------

val lines = ssc.socketTextStream("localhost", 12346)

// COMMAND ----------

val words = lines.map(splitted =>DeviceData(splitted.split(",")(0).toDouble,splitted.split(",")(1).toDouble,splitted.split(",")(2).toInt))

//val final_data ="Transaction: price: " + words.price + ", amount: " +words.amount+", type: "+words.transactiontype


val output = words.map(w=>"New transaction: Price: "+ w.price+", Amount: "+w.amount+", Type: "+ w.transactiontype)

val my_new = output.foreachRDD(rdd =>{ if (!rdd.isEmpty()){rdd.toDF("value").coalesce(1).write.mode("append").format("text").save("file:/tmp/adatok")}})


// COMMAND ----------

ssc.start()