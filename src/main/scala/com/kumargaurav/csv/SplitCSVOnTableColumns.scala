package com.kumargaurav.csv

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, to_timestamp}
import org.apache.spark.sql.types.{DataTypes, DecimalType, LongType}
import org.slf4j.LoggerFactory

import java.util.Date
import java.text.SimpleDateFormat

object SplitCSVOnTableColumns {
  /*val log = LoggerFactory.getLogger(this.getClass.getName)
  val DATE_FORMAT = "yyyy-MM-dd hh:mm:ss"
  def main(args: Array[String]): Unit = {
    val outPath = "/Users/gkumargaur/tmp/tmp"
    val tableName = "itd-aia-datalake.sdwan_landing.tenants"
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .set("materializationDataset","itd-aia-datalake")
    .set("materializationProject","itd-aia-dp")
    .setMaster("local[*]")
    val psstartTime = new Date().getTime
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val record_ingestion_time = dateFormat.format(psstartTime)
    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
    import com.google.cloud.spark.bigquery._
    val df = spark.read.option("parentProject","itd-aia-dp").bigquery(tableName)
    val columnsTodrop = Seq("pubsub_attribute_metadata","retry_attempt_number","record_ingestion_time","request_type")
    //columns to drop based on tables
    //elements
    //val dfact = df.drop(columnsTodrop:_*).withColumnRenamed("creation_date","element_creation_date")
    //machines
    //val dfact = df.drop(columnsTodrop:_*).withColumnRenamed("creation_date","machine_creation_date")
    //tenants
    val dfact = df.drop(columnsTodrop:_*)
    //dfact.show(10)
    val cols = dfact.columns
    val data = spark.read.option("header","true").csv("/Users/gkumargaur/tmp/sap/deviceinventory_2022-05-30.csv")
    val filterData = data.select(cols.map(m=>col(m)):_*)
    filterData.show(10)
    val fldtTemp = filterData.withColumn("retry_attempt_number",lit(0).cast(DataTypes.createDecimalType(4, 0)))
      .withColumn("request_type",lit("NONE"))
      .withColumn("record_ingestion_time", current_timestamp())
      .withColumn("pubsub_attribute_metadata",lit("{\"event_id\":\"1653070290178022296\",\"total_number_message\":\"0\",\"batch_id\":\"1653060917720014300002525654146124496002\",\"message_number\":\"0\",\"event_timestamp\":\"Fri May 20 18:11:30 GMT 2022\",\"tid\":\"301\"}"))
      //.withColumn("creation_date",to_timestamp(col("element_creation_date")).cast(DataTypes.createDecimalType(4, 0))).drop("element_creation_date")
      //.withColumn("creation_date",to_timestamp(col("machine_creation_date")).cast(DecimalType.USER_DEFAULT)).drop("machine_creation_date")
      .withColumn("tenant_creation_date",to_timestamp(col("tenant_creation_date")).cast(DecimalType.USER_DEFAULT))
    val fldt = fldtTemp.dropDuplicates()
    fldt.show(10)
    fldt.repartition(1).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
    .save(outPath)
  }*/
}
