package com.kumargaurav.csv

import com.google.cloud.bigquery.BigQueryOptions
import com.kumargaurav.utils.BQUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object PeopleAI extends App{
  val log = LoggerFactory.getLogger(this.getClass)
  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .set("materializationDataset","itd-aia-datalake")
    .set("materializationProject","itd-aia-dp")
    .setMaster("local[*]")
  implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
  val schemaPath = "/Users/gkumargaur/Downloads/account-export-12_15_222022-03_15_2023.csv"
  log.info("reading file : "+schemaPath)
  val  tbldf = spark.read.option("quote", "\"").
    option("delimiter", ",").
    option("encoding", "UTF-8").
    option("escape", "\\").
    option("nullValue", "\\N").
    option("header", true).csv(schemaPath)
  tbldf.show(10)
  val datalakeprojectId = "itd-aia-datalake"
  val licenseSchemaName = "datapipeline"
  val licensetableName = "vegetables1"
  val licSqlStatement = "select  uid from ite-aia-datalake.people_ai.vw_peopleai_activities b where account_crm_id = '0017000000kYP0BAAW' and DATE(DATETIME(CAST (replace(activity_timestamp, '+00', '') as TIMESTAMP), \"America/Los_Angeles\")) >=  '2022-12-15' and DATE(DATETIME(CAST(replace(activity_timestamp, '+00', '') as TIMESTAMP) , \"America/Los_Angeles\"))  <= '2023-03-15' and lower(b.activity_type) = 'meeting' and CAST(b.external as bool) = true and coalesce(CAST(b.deleted as bool), false) = false and coalesce(CAST(b.is_source_hard_deleted as bool), false)= false group by 1 order by 1 "
  val utils:BQUtils = new BQUtils
  implicit val bigquery = BigQueryOptions.getDefaultInstance.getService
  val licensedf = utils.readBqTable(licSqlStatement, spark, bigquery)
  licensedf.show(10)
}
