package org.gz

import scala.io.Source
import scala.collection.mutable.HashMap
import scala.util.matching.Regex
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import org.apache.commons.text.StringEscapeUtils
import java.io.PrintWriter
import info.monitorenter.cpdetector.io.CodepageDetectorProxy
import info.monitorenter.cpdetector.io.ParsingDetector
import info.monitorenter.cpdetector.io.JChardetFacade
import info.monitorenter.cpdetector.io.ASCIIDetector
import info.monitorenter.cpdetector.io.UnicodeDetector
import org.apache.logging.log4j.LogManager
import com.mongodb.MongoClient
import scala.collection.JavaConversions._
import org.bson.Document
import java.util.ArrayList
import java.util.concurrent.Executors
import com.mongodb.client.model.InsertManyOptions


/**
 * @author cloud
 *	全量数据导入多线程版，但是由于文件读取IO限制，速度并没有比单线程快多少
 */
object ImportOrigin {
	
	val rootPath = "/home/cloud/wenshu/20161231"
	val ls = "D:/library/wenshu/20131231/最高人民法院/0/民事案件/（2013）民二终字第64号_f05d5807-b647-11e3-84e9-5cf3fc0c2c18裁定书.txt"
	val log = LogManager.getLogger(this.getClass.getName())
	lazy val mongo = new MongoClient("192.168.12.161", 27017)
	lazy val db = mongo.getDatabase("wenshu")
	lazy val dbColl = db.getCollection("origin2")

	//探测器
	val det = CodepageDetectorProxy.getInstance()
	det.add(new ParsingDetector(false))
	det.add(JChardetFacade.getInstance)
	det.add(ASCIIDetector.getInstance)
	det.add(UnicodeDetector.getInstance)

	def getAllFiles(path: File) = {
		val files = ArrayBuffer[File]()
		val folders = new ArrayBuffer[File]
		folders += path
		var count = 0
		while (count < folders.length){ 
			if (folders(count).isFile()&&(folders(count).getName.endsWith("txt"))) files += folders(count) else 
				if (folders(count).isDirectory()) folders(count).listFiles().foreach(x => {
					if (x.isFile()&&(x.getName.endsWith("txt"))) files += x else
						if (x.isDirectory()) folders += x
				})
			count = count + 1	
		}
		files
	}
	
	def segByStart(reg: Array[Regex], str: String) = {
		var f = false
		reg.foreach(x => {
			val rs = x.findAllIn(str.trim)
  		if (rs.hasNext)
  			if (rs.start < 2)
  				f = true
  	})
  	f
	}
	
	def segByAll(reg: Array[Regex], str: String) = {
		var f = false
		reg.foreach(x => {
			val rs = x.findAllIn(str)
  		if (rs.hasNext)
  				f = true
  	})
  	f
	}
	
	def getwenshuID(fileName: String) = {
		val n = fileName.split("_")
		val name = n(n.length - 1)
		val reg = "[0-9a-z\\-]+".r
		reg.findFirstIn(name) match {
			case Some(s) => s
			case None => ""
		}
	}
	
	def filterHtml(arr: Array[String]) = {
		var ssss = StringEscapeUtils.unescapeHtml4(arr.mkString("\n"))
  	val rx = "<[\\W\\w]*?>".r
  	val rx_redundancy = raw"<\S+?:.*?>.*?</\S+?:.*?>".r
  	val rx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>".r
  	val rx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>".r
  	ssss = rx_script.replaceAllIn(ssss, "")
  	ssss = rx_style.replaceAllIn(ssss, "")
  	ssss = rx_redundancy.replaceAllIn(ssss, "")
  	ssss = rx.replaceAllIn(ssss, "")
  	ssss.replaceAll("((\r\n)|\n)[\\s\t ]*(\\1)+", "$1").replaceAll("^((\r\n)|\n)", "").split("((\r\n)|\n)")
	}
	
	def detector(f: File) = {
		var cs: java.nio.charset.Charset = null
		try{
			cs = det.detectCodepage(f.toURI.toURL)
		}catch {
			case e: Throwable => log.error(e)
		}
		log.info(cs.toString() + ":\t" + f.getName)
		cs.toString() match {
			case "UTF-8" => "UTF-8"
			case _ => "GBK"
		}
	}
	
  def main(args: Array[String]): Unit = {
  	val rootFolder = new File(rootPath)
  	val scheduler = Executors.newFixedThreadPool(8)
  	rootFolder.listFiles.foreach(y => {
  		val r = new Runnable(){
  			override def run(): Unit = {
  				var resList = new ArrayList[Document]
  				var count = 0
		  		y.listFiles().foreach(x => {
		  		log.warn("new path start:" + x.getPath)		  		
		  		log.warn("GetAllFiles start: " + x.getPath)
		  		val files = getAllFiles(x)
		  		log.warn("GetAllFiles done: " + x.getPath)
		 			files.foreach(x => 
			 			try{
			 				log.info("start wenshu chuli:" + x.getPath)
		 					var d = new Document
							val id = getwenshuID(x.getName)
							if (id != "")	d.append("_id", id)
							d.append("path", x.getPath)
							d.append("content", filterHtml(Source.fromFile(x, detector(x)).getLines().toArray).mkString("\n"))
							resList.add(d)
							count = count+1
							log.info("end wenshu chuli:" + x.getPath)
							if (count == 10000) {
								log.warn("start insert 10000:")
								dbColl.insertMany(resList)
								count = 0
								resList.clear
								log.warn("finish insert 10000:")
							}
		 				}catch {
			 				case e: Throwable => log.error(e)
		 				})
		 			log.warn("All done: " + x.getPath)
		 			})
		 		dbColl.insertMany(resList)
  			}
  		}
  		scheduler.execute(r)
  	})
  }
}