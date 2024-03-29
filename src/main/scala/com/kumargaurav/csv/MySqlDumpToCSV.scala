package com.kumargaurav.csv

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object MySqlDumpToCSV extends App {
  val log = LoggerFactory.getLogger(this.getClass)
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .set("materializationDataset","itd-aia-datalake")
    .set("materializationProject","itd-aia-dp")
    .setMaster("local[*]")
  implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
  val schemaPath = if(args.length>0 && args(0) != "" ) args(0) else "gs://it-slr-historical-data-qa/sample.txt"
  log.info("reading file : "+schemaPath)
  val  tbldf = spark.read.option("quote", "\"").
    option("delimiter", "|").
    option("encoding", "UTF-8").
    option("escape", "\\").
    option("nullValue", "\\N").
    option("header", true).csv(schemaPath)
  tbldf.show(100)
  tbldf.write.mode(SaveMode.Overwrite).parquet("gs://itd-aia-de/temp/virusdb/")
}
