package com.kumargaurav.avro
import java.io.File
object FileReadTest {
  def main(args: Array[String]): Unit = {
    val extensions = List("-value.avsc");
    val readPath = new File("/Users/gkumargaur/Downloads/infolease")
    val files = getListOfFiles(readPath, extensions)
    println(files)
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }
}
