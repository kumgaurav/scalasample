package com.kumargaurav.csv.virus

import com.google.cloud.bigquery.BigQueryOptions
import com.kumargaurav.csv.virus.SigEventCSVToTable.args
import com.kumargaurav.utils.BQUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.slf4j.LoggerFactory

object CSVFileReadTest extends App{
  val log = LoggerFactory.getLogger(this.getClass)
  val utils:BQUtils = new BQUtils
  val datalakeprojectId = "itd-aia-datalake"
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .set("materializationDataset","itd-aia-datalake")
    .set("materializationProject","itd-aia-dp")
    .setMaster("local[*]")
  implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
  implicit val bigquery = BigQueryOptions.getDefaultInstance.getService
  val schemaPath = if(args.length>0 && args(0) != "" ) args(0) else "/Users/gkumargaur/tmp/sap/signature.txt"
  log.info("reading file : "+schemaPath)
  val  tbldf = spark.read
    .option("quote", "\"")
   .option("delimiter", "|").
    option("encoding", "UTF-8").
    option("escape", "\\").
    option("nullValue", "\\N").
    option("escapeQuotes",true).
    option("header", true).csv(schemaPath)
  val csvdf = tbldf
    //.withColumn("action", lit("NONE"))
    //.withColumn("metadata",lit("NONE"))
    //.withColumn("pubsub_attribute_metadata",lit("{\"manual_load\": true}"))
    //.withColumn("retry_attempt_number",lit(0))
    .withColumn("record_ingestion_time", current_timestamp())
  csvdf.show(100)
  //val sql = " SELECT * FROM `"+datalakeprojectId+".virus_landing.sample` limit 1 "
  //val baxterdf = utils.readBqTable(sql, spark, bigquery)
  //val columnsAll=baxterdf.columns.map(m=>col(m))
  val columns = Seq("pubsub_attribute_metadata","retry_attempt_number","record_ingestion_time","severity","min_version","build_version","type","update_date","sid","max_version","xml","wildfire","name","category","create_date","status","regression_time","action","metadata")
  //val finaldf = csvdf.select(columns.map(c => col(c)): _*)
  //finaldf.show(100)
}
