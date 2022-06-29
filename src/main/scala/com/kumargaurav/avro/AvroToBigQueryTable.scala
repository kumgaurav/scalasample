package com.kumargaurav.avro

import org.apache.spark.sql.avro.SchemaConverters
import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import com.google.cloud.spark.bigquery._
import com.google.auth.oauth2.AccessToken
import com.kumargaurav.MiscUtil
import org.apache.spark.sql.functions.current_date
import org.json4s._
import org.json4s.jackson.JsonMethods._

object AvroToBigQueryTable {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val projectId = "itd-aia-dp"
    val datalakeprojectId = "itd-aia-datalake"
    val outSchemaName = "solifi_landing"
    val staging_bucket: String = if (args.length > 0 && args(0) != "") args(0) else "gs://itd-aia-de-dproc-staging/solifi/ls_billing_nf-value.avsc"
    val gcsBucket: String = staging_bucket.substring(staging_bucket.indexOf("gs://") + 5, staging_bucket.lastIndexOf('/'))
    println("gcsBucket -> " + gcsBucket)
    val schemaPath = staging_bucket
    implicit val accessToken: AccessToken = MiscUtil.getAccessToken("itd-aia-solifi@itd-aia-dp.iam.gserviceaccount.com")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
      .set("materializationDataset", "itd-aia-datalake")
      .set("materializationProject", "itd-aia-dp")
      .set("gcpAccessToken", accessToken.getTokenValue)
      .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate
    println("reading file -> " + schemaPath)
    val rdd = spark.sparkContext.wholeTextFiles(schemaPath)
    val schemaText = rdd.take(1)(0)._2
    println("schemaText -> " + schemaText)
    val json = parse(schemaText).transformField { case JField(k, v) => JField(k.toLowerCase, v) }
    // Converting from JOjbect to plain object
    implicit val formats = DefaultFormats
    val tableName = (json \ "name").extract[String]
    println("tableName -> "+tableName)
    val tableNameForWrite = datalakeprojectId + "." + outSchemaName + "." + tableName
    println("tableNameForWrite -> "+tableNameForWrite)
    val schema = new Schema.Parser().parse(schemaText)
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule("org.apache.spark.sql.avro.SchemaConverters")
    val im = m.reflectModule(module)
    val method = im.symbol.info.decl(ru.TermName("toSqlType")).asMethod
    val objMirror = m.reflect(im.instance)
    val structure = objMirror.reflectMethod(method)(schema).asInstanceOf[org.apache.spark.sql.avro.SchemaConverters.SchemaType]
    val sqlSchema = structure.dataType.asInstanceOf[StructType]
    val empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sqlSchema)
    val finaldf = empty_df.withColumn("record_ingestion_date",current_date())
    finaldf.printSchema()
    finaldf.write.format("bigquery")
      .mode(SaveMode.Overwrite)
      .option("writeMethod", "direct")
      .option("temporaryGcsBucket", gcsBucket)
      .option("parentProject", projectId)
      .option("createDisposition", "CREATE_IF_NEEDED")
      .option("clusteredFields", "id")
      .save(tableNameForWrite)
    spark.stop()
  }

}
