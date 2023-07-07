package com.kumargaurav.parquet

import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, FormatOptions, Job, JobInfo, LoadJobConfiguration, TableId}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object ParquetToBigQuery {
  val log = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val projectId = "itd-aia-dp"
    val datalakeprojectId = "itd-aia-datalake"
    val outSchemaName = "sap_inbound_rpa"
    val tableName = "zinvcm"
    val gcsLocation = "gs://itd-aia-de/temp/zinc/"
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
      .set("materializationDataset", "itd-aia-datalake")
      .set("materializationProject", "itd-aia-dp")
      .setMaster("local[*]")
    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
    implicit val bigquery: BigQuery = BigQueryOptions.newBuilder()
      .setProjectId(projectId)
      //.setCredentials(GoogleCredentials.fromStream(credentialsStream))
      .build().getService
    val tableIdFull:TableId = TableId.of(datalakeprojectId,outSchemaName, tableName)
    println("Table for write ========================"+tableIdFull.toString)
    val configurationFull : LoadJobConfiguration = LoadJobConfiguration.builder(tableIdFull,gcsLocation+"*.parquet")
      .setFormatOptions(FormatOptions.parquet())
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .build()
    var loadJobFull : Job = bigquery.create(JobInfo.of(configurationFull))
    loadJobFull = loadJobFull.waitFor()
  }
}
