package com.kumargaurav.json

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.{col, lit, row_number}

import scala.collection.JavaConverters.mapAsScalaMapConverter

object JsonToCsv extends App {
  val log = LoggerFactory.getLogger(this.getClass)
  val outPath = "/Users/gkumargaur/tmp/tmp"
  val tableName = "sdwan.elements"
  log.info("Scala version -> " + scala.util.Properties.versionString)
  //println(System.getenv().asScala("JAVA_OPTS"))
  println(System.getenv().asScala("JAVA_HOME"))
  //val environmentVars = System.getenv().asScala
  //for ((k,v) <- environmentVars) println(s"key: $k, value: $v")
  //val properties = System.getProperties().asScala
  //for ((k,v) <- properties) println(s"key: $k, value: $v")
  //println(System.getProperties().asScala("java.version"))
  val jsonPath = "/Users/gkumargaur/workspace/java/office/pub-sub-to-bq-dataflow/datasets/target/sdwan_landing/tables/elements.json"
  val spark = SparkSession.builder().master("local[*]").appName("hello").getOrCreate()
  log.info("Spark version -> "+spark.sparkContext.version)
  log.info("Scala version -> " + scala.util.Properties.versionString)
  val df = spark.read.option("multiline", "true").json(jsonPath)
  //df.show(10)
  import spark.implicits._
  val df1 = df.withColumn("TableName",lit(tableName))
  val window = Window.partitionBy("TableName").orderBy(col("name"))
  val tbdf = df1.withColumn("row_num", row_number().over(window)).select("row_num","TableName","name","type")
  tbdf.show(10)
  tbdf.repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(outPath)
  spark.close()
}