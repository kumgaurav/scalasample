package com.kumargaurav.utils

import com.google.cloud.bigquery.{BigQuery, JobId, QueryJobConfiguration}
import org.apache.spark.sql.SparkSession

class BQUtils {
  def readBqTable(credentialsStreamFilePath: String,sqlStatement: String, spark: SparkSession, bigquery: BigQuery) = {
    val temp_table = bqReadView(sqlStatement, bigquery)
    val df =spark.read.format("bigquery")
      .option("table", temp_table._1+":"+temp_table._2+"."+temp_table._3)
      //.option("credentialsFile", credentialsStreamFilePath)
      .load()
    //df.show(2)
    df
  }

  def readBqTable(sqlStatement: String, spark: SparkSession, bigquery: BigQuery) = {
    val temp_table = bqReadView(sqlStatement, bigquery)
    val df =spark.read.format("bigquery")
      .option("table", temp_table._1+":"+temp_table._2+"."+temp_table._3)
      .load()
    //df.show(2)
    df
  }

  private def bqReadView(sqlStatement: String, bigquery: BigQuery) = {
    val queryConfig   = QueryJobConfiguration.newBuilder(
      sqlStatement)
      .setUseLegacySql(false) //default is false
      //.setCreateDisposition() default is CREATE_IF_NEEDED
      .build()
    val jobId: JobId = JobId.of( java.util.UUID.randomUUID.toString )
    val tblResult = bigquery.query( queryConfig, jobId)
    tblResult.iterateAll()
    val queryJobConfig = bigquery.getJob(jobId).getConfiguration.asInstanceOf[QueryJobConfiguration].getDestinationTable
    val cacheDataset:String = queryJobConfig.getDataset
    val cacheTable:String = queryJobConfig.getTable
    val cacheProject:String = queryJobConfig.getProject
    (cacheProject,cacheDataset,cacheTable)
  }
}
