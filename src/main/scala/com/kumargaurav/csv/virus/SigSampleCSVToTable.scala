package com.kumargaurav.csv.virus

import com.google.cloud.bigquery.BigQueryOptions
import com.kumargaurav.utils.BQUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object SigSampleCSVToTable extends App {
    val log = LoggerFactory.getLogger(this.getClass)
    val utils:BQUtils = new BQUtils
    val datalakeprojectId = "itd-aia-datalake"
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
      .set("materializationDataset","itd-aia-datalake")
      .set("materializationProject","itd-aia-dp")
      .setMaster("local[*]")
    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
    implicit val bigquery = BigQueryOptions.getDefaultInstance.getService
    val schemaPath = if(args.length>0 && args(0) != "" ) args(0) else "gs://it-slr-historical-data-qa/sample.txt"
    log.info("reading file : "+schemaPath)
    val  tbldf = spark.read.option("quote", "\"").
      option("delimiter", "|").
      option("encoding", "UTF-8").
      option("escape", "\\").
      option("nullValue", "\\N").
      option("header", true).csv(schemaPath)
     val csvdf = tbldf.withColumn("action", lit("NONE"))
      .withColumn("metadata",lit("NONE"))
       .withColumn("pubsub_attribute_metadata",lit("{\"manual_load\": true}"))
       .withColumn("retry_attempt_number",lit(0))
      .withColumn("record_ingestion_time", current_timestamp())
    //val sql = " SELECT * FROM `"+datalakeprojectId+".virus_landing.sample` limit 1 "
    //val baxterdf = utils.readBqTable(sql, spark, bigquery)
    //val columnsAll=baxterdf.columns.map(m=>col(m))
    val columns = Seq("pubsub_attribute_metadata","retry_attempt_number","record_ingestion_time","mid","id","sid","action","metadata")
    val finaldf = csvdf.select(columns.map(c => col(c)): _*)
    finaldf.show(100)
    finaldf.write.mode(SaveMode.Overwrite).parquet("gs://itd-aia-de/temp/virusdb/sig_sample/")
}
