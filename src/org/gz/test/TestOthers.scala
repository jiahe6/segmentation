package org.gz.test

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.gz.ImportOrigin
import scala.io.Source
import java.io.File

object TestOthers {
  def main(args: Array[String]): Unit = {
  	val file = new File("C:/Users/cloud/Desktop/（2015）包民一终字第311号_c2b5d017-0ec8-4563-a398-a78e00f5436d判决书.txt")
    val res = ImportOrigin.filterHtml2(Source.fromFile(file, "gbk").getLines().toArray.map(x => {x.replaceAll("\u0000", "").trim()}))
    //res.foreach { println}
    println(res)
  }
}