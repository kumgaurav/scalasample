package com.kumargaurav.avro

import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, FormatOptions, Job, JobInfo, LoadJobConfiguration, TableId}

import scala.reflect.runtime.{universe => ru}
import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_timestamp, from_unixtime}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.json4s.{DefaultFormats, FieldSerializer}
import org.json4s.FieldSerializer.{ignore, renameFrom, renameTo}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Date
import scala.collection.mutable

object AvroToParquet {
  val log = LoggerFactory.getLogger(this.getClass)
  case class Fields(name : String, _type:Any )
  case class AvroMessage(_type:String, name : String, namespace : String,fields:List[Fields] )
  def main(args: Array[String]): Unit = {
    val projectId = "itd-aia-dp"
    val datalakeprojectId = "itd-aia-datalake"
    val outSchemaName = "solifi_landing"
    val outLocation = "gs://itd-aia-dp-dproc-staging/solifi/"
    val extensions = List("-value.avsc");
    val readPath = new File("/Users/gkumargaur/tmp/solifi")
    val files = getListOfFiles(readPath, extensions)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local")
      .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val bigquery: BigQuery = BigQueryOptions.newBuilder()
      .setProjectId(projectId)
      .build().getService
    log.info("Scala version -> "+scala.util.Properties.versionString)
    log.info("Spark version -> "+spark.sparkContext.version)
    try{
      for( schemaPath <- files) {
        val startTime = System.currentTimeMillis()
        println(schemaPath)
        val rdd = spark.sparkContext.wholeTextFiles(schemaPath.getPath)
        val schemaText = rdd.take(1)(0)._2
        println("schemaText -> " + schemaText)
        var (tableName, dateCols) = parseAvro(schemaText)
        println("dateCols -> " + dateCols)
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
        val finaldf = bulkColumnLongToDate(tempdf, dateCols)
        finaldf.printSchema()
        finaldf.write.format("parquet").mode(SaveMode.Overwrite).save(outLocation)
        val tableId: TableId = TableId.of(datalakeprojectId, outSchemaName, tableName)
        println("Table for write ========================" + tableId.toString)
        val configuration: LoadJobConfiguration = LoadJobConfiguration.builder(tableId, outLocation + "*.parquet")
          .setFormatOptions(FormatOptions.parquet())
          .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
          .build()
        val loadJobPre: Job = bigquery.create(JobInfo.of(configuration))
        val loadJobSucess = loadJobPre.waitFor()
        if (loadJobSucess == null) throw new RuntimeException("Job no longer exists")
        else if (loadJobSucess.getStatus.getError != null) {
          // You can also look at queryJob.getStatus().getExecutionErrors() for all
          // errors, not just the latest one.
          log.error("loadJobSucess -> " + loadJobSucess.getStatus.getError)
          log.error("loadJobSucess Str-> " + loadJobSucess.getStatus.getError.getMessage)
          val errMgs = "ErMsg 1 : -> " + loadJobSucess.getStatus.getError.getMessage
          throw new RuntimeException(errMgs)
        } else if (loadJobPre.getStatus.getError != null) {
          log.error("loadJobPre -> " + loadJobPre.getStatus.getError)
          log.error("loadJobPre Str-> " + loadJobPre.getStatus.getError.getMessage)
          val errMgs = "ErMsg 2 : -> " + loadJobPre.getStatus.getError.getMessage
          throw new RuntimeException(errMgs)
        }
        val endtime = System.currentTimeMillis()
        log.info("Finished writing to ----- " + tableName + ", Total time taken -> " + (endtime - startTime))
        schemaPath.delete()
      }
    }catch {
      case e: Exception =>
        log.error(e.getMessage)
        e.printStackTrace()

    }
  }

  def bulkColumnLongToDate(df:DataFrame, cols: Seq[String] = Nil): DataFrame = {
    cols.foldLeft(df)((acc, c) => acc.withColumn(c, from_unixtime(col(c),"yyyy-MM-dd").cast(DataTypes.DateType)))
  }

  def parseAvro(schemaText : String ) : (String , mutable.MutableList[String])  ={
    var dateCols = mutable.MutableList[String]()
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
    //val avroMessage = read[AvroMessage](schemaText)
    //val name = (json \ "name").extract[String]
    val tableName = avroMessage.name
    println("tableName -> "+tableName)
    //println("logicalType -> "+avroMessage._type)
    for (obj <- avroMessage.fields.filter(_._type.getClass.getName.contains("scala.collection.immutable"))) {
      if(obj._type.isInstanceOf[List[Any]]) {
        val typeLst = obj._type.asInstanceOf[List[Any]]
        //println("typeLst(1) -> "+typeLst(1)+", "+typeLst(1).getClass)
        val typeMap = if (typeLst.size == 2 && typeLst(1).getClass.getName.contains("scala.collection.immutable.Map")) typeLst(1).asInstanceOf[Map[String, String]] else Map[String, String]()
        if (typeMap.getOrElse("logicalType", "").toString == "date") {
          //println("found -> "+obj.name)
          dateCols += obj.name
          //println(obj.name+", found -> "+dateCols)
        }
      }
      //println("type2 -> "+obj.name +" , "+obj._type+", "+obj._type.getClass + ", "+obj._type.getClass.getName)
      if(obj._type.getClass.getName.contains("Map")){
        val typeMap = obj._type.asInstanceOf[Map[String,String]]
        if(typeMap.getOrElse("logicalType", "").toString == "date"){
          dateCols += obj.name
          //println(obj.name+", found -> "+dateCols)
        }
      }
    }
    (tableName,dateCols)
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }
}
