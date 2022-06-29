package com.kumargaurav.avro


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

object AvroReadTest {
  val log = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val schemaPath = if(args.length>0 && args(0) != "" ) args(0) else "/Users/gkumargaur/tmp/solifi/ls_billing_nf-value.avsc"
    //try{
    val rdd = spark.sparkContext.wholeTextFiles(schemaPath)
    val schemaText = rdd.take(1)(0)._2
    println("schemaText -> " + schemaText)
    val json = parse(schemaText).transformField { case JField(k, v) => JField(k.toLowerCase, v) }
    // Converting from JOjbect to plain object
    implicit val formats = DefaultFormats
    val name = (json \ "name").extract[String]
    println(name)
    //}
    spark.stop()
  }
}
