package com.kumargaurav.avro

import scala.reflect.runtime.{universe => ru}
import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_timestamp, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.json4s._
import org.json4s.FieldSerializer.{ignore, renameFrom, renameTo}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object AvroReadTest {
  val log = LoggerFactory.getLogger(this.getClass)
  case class Fields(name : String, _type:Any )
  case class AvroMessage(_type:String, name : String, namespace : String,fields:List[Fields] )

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    log.info("Scala version -> "+scala.util.Properties.versionString)
    log.info("Spark version -> "+spark.sparkContext.version)
    val schemaPath = if(args.length>0 && args(0) != "" ) args(0) else "/Users/gkumargaur/tmp/solifi/ls_blended_inc_bi_tax_table-value.avsc"
    //try{
    val rdd = spark.sparkContext.wholeTextFiles(schemaPath)
    val schemaText = rdd.take(1)(0)._2
    println("schemaText -> " + schemaText)

    //val json = parse(schemaText).transformField { case JField(k, v) => JField(k.toLowerCase, v) }
    //val json = parse(schemaText)


    //val fields = (json \ "fields").extract[String]
    //println("fields -> "+fields)
    //val avroMessage = parse(fields).extract[AvroMessage]
    //val logicalType = json filterField {
    //  case JField("name", JString("last_pymt_date"))   => true
    //  case _ => false
    //}
    var (tableName,dateCols) = parseAvro(schemaText)
    println("dateCols -> "+dateCols)
    val schema = new Schema.Parser().parse(schemaText)
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule("org.apache.spark.sql.avro.SchemaConverters")
    val im = m.reflectModule(module)
    val method = im.symbol.info.decl(ru.TermName("toSqlType")).asMethod
    val objMirror = m.reflect(im.instance)
    val structure = objMirror.reflectMethod(method)(schema).asInstanceOf[org.apache.spark.sql.avro.SchemaConverters.SchemaType]
    val sqlSchema = structure.dataType.asInstanceOf[StructType]
    val empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sqlSchema)
    val tempdf = empty_df.withColumn("record_ingestion_time",current_timestamp())
    val finaldf = bulkColumnLongToDate(tempdf, dateCols)
    finaldf.printSchema()
    finaldf.write.format("parquet").mode(SaveMode.Overwrite).save("/Users/gkumargaur/tmp/sap")
    //}
    spark.stop()
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
    //println("_._type.getClass.getName "+_._type.getClass.getName)
    for (obj <- avroMessage.fields.filter(_._type.getClass.getName.contains("scala.collection.immutable"))) {
      println("type -> "+obj.name +" , "+obj._type+", "+obj._type.getClass + ", "+obj._type.getClass.getName)
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
        println("1 inside if : ")
        val typeMap = obj._type.asInstanceOf[Map[String,String]]
        println("2 inside if : "+typeMap)
        println("3 inside if : "+typeMap.getOrElse("logicalType","no key"))
        if(typeMap.getOrElse("logicalType", "").toString == "date"){
          dateCols += obj.name
          println(obj.name+", found -> "+dateCols)
        }
      }
    }
    (tableName,dateCols)
  }
}
