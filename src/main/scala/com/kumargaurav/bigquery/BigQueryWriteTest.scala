package com.kumargaurav.bigquery
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object BigQueryWriteTest extends App {
  /*val log = LoggerFactory.getLogger(this.getClass)
  val projectId = "itd-aia-dp"
  val staging_bucket = "itd-aia-dp-dproc-staging/solifi"
  //println(System.getenv().asScala("JAVA_OPTS"))               // -Dfile.encoding=UTF-8
  println(System.getProperties().asScala("java.version"))     // 11.0.7
  println(System.getProperties().asScala("java.vm.version"))
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .set("spark.jars","/Users/gkumargaur/workspace/scala/personal/scalasample/spark-2.4-bigquery-0.25.2-preview.jar")
    //.set("temporaryGcsBucket", staging_bucket)
    .setMaster("local[*]")
  implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
  //spark.conf.set("temporaryGcsBucket", bucket)
  import com.google.cloud.spark.bigquery._
  val wordsDF = spark.read.option("parentProject", projectId).bigquery("bigquery-public-data.samples.shakespeare")
  wordsDF.show()
  wordsDF.printSchema()
  wordsDF.createOrReplaceTempView("words")

  // Perform word count.
  val wordCountDF = spark.sql(
    "SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word")
  println("--------------------------------------")
  // Saving the data to BigQuery
  //wordCountDF.write.option("writeMethod", WriteMethod.DIRECT.toString).option("parentProject", projectId).bigquery("wordcount_dataset.wordcount_output")
  wordCountDF.write.format("bigquery")
    .option("writeMethod", "direct")
    .option("enableModeCheckForSchemaFields", false)
    .option("parentProject", projectId)
    .save("wordcount_dataset.wordcount_output")*/
}
