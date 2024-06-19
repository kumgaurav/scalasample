package com.kumargaurav.avro

import org.json4s._
import org.json4s.FieldSerializer.{ignore, renameFrom, renameTo}
import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.slf4j.LoggerFactory
import com.google.auth.oauth2.AccessToken
import com.kumargaurav.MiscUtil
import org.apache.spark.sql.functions.{col, current_timestamp, from_unixtime}


import scala.collection.mutable

object AvroToBigQueryTable {
  val log = LoggerFactory.getLogger(this.getClass)
  case class Fields(name : String, _type:Any ) extends Serializable
  case class AvroMessage(_type:String, name : String, namespace : String,fields:List[Fields] ) extends Serializable
  def main(args: Array[String]): Unit = {
    val projectId = "itd-aia-dp"
    val datalakeprojectId = "itd-aia-datalake"
    val outSchemaName = "solifi_landing"
    val staging_bucket: String = if (args.length > 0 && args(0) != "") args(0) else "gs://itd-aia-de-dproc-staging/solifi/ls_blended_inc_bi_tax_table-value.avsc" //ls_blended_inc_bi_tax_table-value.avsc,ls_billing_nf-value.avsc
    val cluster_column: String = if (args.length > 1 && args(1) != "") args(1) else "id"
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
    try {
      log.info("Scala version -> "+scala.util.Properties.versionString)
      log.info("Spark version -> "+spark.sparkContext.version)

      println("reading file -> " + schemaPath)
      val rdd = spark.sparkContext.wholeTextFiles(schemaPath)
      val schemaText = rdd.take(1)(0)._2
      println("schemaText -> " + schemaText)
      var (tableName,dateCols) = parseAvro(schemaText)
      println("tableName -> " + tableName)
      val tableNameForWrite = datalakeprojectId + "." + outSchemaName + "." + tableName
      println("tableNameForWrite -> " + tableNameForWrite)
      val schema = new Schema.Parser().parse(schemaText)
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val module = m.staticModule("org.apache.spark.sql.avro.SchemaConverters")
      val im = m.reflectModule(module)
      val method = im.symbol.info.decl(ru.TermName("toSqlType")).asMethod
      val objMirror = m.reflect(im.instance)
      val structure = objMirror.reflectMethod(method)(schema).asInstanceOf[org.apache.spark.sql.avro.SchemaConverters.SchemaType]
      val sqlSchema = structure.dataType.asInstanceOf[StructType]
      val empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sqlSchema)
      val tempdf = empty_df.withColumn("record_ingestion_time", current_timestamp())
      println("dateCols -> " + dateCols)
      var finaldf = bulkColumnLongToDate(tempdf, dateCols)
      // Check if the DataFrame is empty and add a dummy row if it is
      if (finaldf.isEmpty) {
        val dummyRow = finaldf.schema.fields.map(_.dataType match {
          case DataTypes.StringType => "dummy"
          case DataTypes.IntegerType => 0
          case DataTypes.LongType => 0L
          case DataTypes.DoubleType => 0.0
          case DataTypes.DateType => java.sql.Date.valueOf("1970-01-01")
          case DataTypes.TimestampType => new java.sql.Timestamp(0L)
          case _ => null
        })

        val dummyDF = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(dummyRow: _*))), finaldf.schema)
        finaldf = finaldf.union(dummyDF)
      }
      finaldf.printSchema()
      finaldf.show()
      finaldf.write.format("bigquery")
        .mode(SaveMode.Overwrite)
        .option("writeMethod", "direct")
        .option("temporaryGcsBucket", gcsBucket)
        .option("parentProject", projectId)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("clusteredFields", cluster_column)
        .save(tableNameForWrite)
    }catch {
      case e: Exception =>
        log.error(e.getMessage,e)
    }
    spark.stop()
  }
  def bulkColumnLongToDate(df:DataFrame, cols: Seq[String] = Nil): DataFrame = {
    cols.foldLeft(df)((acc, c) => acc.withColumn(c, from_unixtime(col(c),"yyyy-MM-dd").cast(DataTypes.DateType)))
  }

  def parseAvro(schemaText : String ) : (String , mutable.MutableList[String])  ={
    var dateCols = mutable.MutableList[String]()
    //implicit val mf: Formats = DefaultFormats
    val avroSerializer = FieldSerializer[AvroMessage](
      renameTo("type", "_type") orElse ignore("owner"),
      renameFrom("type", "_type"),true)
    val fieldsSer = FieldSerializer[Fields](
      renameTo("type", "_type") orElse ignore("owner"),
      renameFrom("type", "_type"), true)
    // Converting from JOjbect to plain object
    implicit val formats = DefaultFormats.strict + avroSerializer + fieldsSer
    import org.json4s.jackson.JsonMethods._
    val avroMessage = parse(schemaText, true).extract[AvroMessage]
    //val name = (json \ "name").extract[String]
    val tableName = avroMessage.name
    println("tableName -> "+tableName)
    //println("logicalType -> "+avroMessage._type)
    for (obj <- avroMessage.fields.filter(_._type.getClass.getName.contains("scala.collection.immutable"))) {
      //println("type -> "+obj.name +" , "+obj._type+", "+obj._type.getClass + ", "+obj._type.getClass.getName)
      val typeLst = obj._type.asInstanceOf[List[Any]]
      //println("typeLst(1) -> "+typeLst(1)+", "+typeLst(1).getClass)
      val typeMap = if(typeLst.size == 2 && typeLst(1).getClass.getName.contains("scala.collection.immutable.Map")) typeLst(1).asInstanceOf[Map[String, String]] else Map[String, String]()
      if(typeMap.getOrElse("logicalType","").toString == "date"){
        //println("found -> "+obj.name)
        dateCols += obj.name
        //println(obj.name+", found -> "+dateCols)
      }
    }
    (tableName,dateCols)
  }
}
