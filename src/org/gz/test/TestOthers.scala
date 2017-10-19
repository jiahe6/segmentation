package org.gz.test

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.sys.process._
import org.gz.ImportOrigin
import scala.io.Source
import java.io.File
import org.gz.data.importwenshu.ScheduleImport
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.commons.compress.archivers.zip.ZipFile

object TestOthers {
  def main(args: Array[String]): Unit = {
//  	println("start testOthers")
//  	val file = new File("/home/cloud/（2015）包民一终字第311号_c2b5d017-0ec8-4563-a398-a78e00f5436d判决书.txt")
//  	println(file.getPath)
//  	val arr = Source.fromFile(file).getLines().toArray  	
//  	println("--------------------------------------------")
//  	println("read done")
//    val res = ImportOrigin.filterHtml3("/home/cloud/（2015）包民一终字第311号_c2b5d017-0ec8-4563-a398-a78e00f5436d判决书.txt")
//    //res.foreach { println}
//    println(res)
/*  	val scheduler = Executors.newScheduledThreadPool(2)
  	val r = new Runnable(){
  		override def run(): Unit = {
    		ScheduleImport.DownloadRar("20170609")
  		}
    }
  	val r2 = new Runnable(){
  		override def run(): Unit = {
  		}
    }
  	scheduler.scheduleAtFixedRate(r, 0, 1, TimeUnit.HOURS)*/
  	
  	val f = new File("D:/library/wenshu/20140407.rar")
		val zipFile = new ZipFile(f, "GBK")
//		var entries = zipFile.getEntries
  	
  	
  }
  
}