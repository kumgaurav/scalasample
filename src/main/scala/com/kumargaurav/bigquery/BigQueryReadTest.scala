package com.kumargaurav.bigquery


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

object BigQueryReadTest extends App {
 /* val log = LoggerFactory.getLogger(this.getClass)
  val projectId = "itd-aia-dp"
  //println(System.getenv().asScala("JAVA_OPTS"))               // -Dfile.encoding=UTF-8
  println(System.getProperties().asScala("java.version"))     // 11.0.7
  println(System.getProperties().asScala("java.vm.version"))
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .set("spark.jars","/Users/gkumargaur/workspace/scala/personal/scalasample/spark-bigquery-latest_2.12.jar")
    .setMaster("local[*]")
  implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
  import com.google.cloud.spark.bigquery._
  val df = spark.read.option("parentProject", projectId).bigquery("bigquery-public-data.samples.shakespeare")
  df.show(10)*/
}
