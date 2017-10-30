package org.gz

import scala.io.Source
import scala.collection.mutable.HashMap
import scala.util.matching.Regex
import scala.sys.process._
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
import com.mongodb.client.MongoCollection
import org.gz.data.importwenshu.ImportDataProcess


/**
 * @author cloud
 *	全量数据导入多线程版，但是由于文件读取IO限制，速度并没有比单线程快多少
 */
object ImportOrigin {
	
	val rootPath = "/home/cloud/wenshu/20161231"
	val ls = "D:/library/wenshu/20131231/最高人民法院/0/民事案件/（2013）民二终字第64号_f05d5807-b647-11e3-84e9-5cf3fc0c2c18裁定书.txt"
	val log = LogManager.getLogger(this.getClass.getName())
	lazy val mongo = new MongoClient("192.168.12.161", 27017)
	lazy val db = mongo.getDatabase("updatesdata")
	lazy val dbColl = db.getCollection("newdata")

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
	
	def getwenshuID(fileName: String) = {
		val n = fileName.split("_")
		val name = n(n.length - 1)
		val reg = "[0-9a-z\\-]+".r
		reg.findFirstIn(name) match {
			case Some(s) => s
			case None => ""
		}
	}
	
	//过滤html标签，但是又找不到那几个奇怪的文书了，所以先注释一句
	//这样直接去掉<>里的内容会去掉法规名称，有点怪
	//用python的htmlParser倒是可以完美去掉标签，可是java调python不好调
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
  	ssss.replaceAll("((\r\n)|\n)[\\s\t ]*(\\1)+", "$1").replaceAll("\u0000", "").replaceAll("^((\r\n)|\n)", "").split("((\r\n)|\n)")  	
	}
	
	//不大好用？
	def filterHtml2(arr: Array[String]) = {
		var ssss = StringEscapeUtils.unescapeHtml4(arr.mkString("\n"))		    
    // 遍历所有的节点
		val parser = org.htmlparser.Parser.createParser(new String(ssss.getBytes()), "UTF-8");
    val nodes = parser.extractAllNodesThatMatch(new org.htmlparser.NodeFilter() {
        override def accept(node: org.htmlparser.Node) = {
          true
        }
    })
    var res = new StringBuffer()
    System.out.println(nodes.size); //打印节点的数量
    for (i <- 0 until nodes.size){
    	val nodet = nodes.elementAt(i);
      println(nodet.getText()); 
      res.append(new String(nodet.toPlainTextString().getBytes("UTF-8"))+"/n");          
    }
    res.toString
	}
	
	//加入了一个python过滤标签的方法
	def filterHtml3(path: String) = {
		println("-------------------------------------------------------")		
		val str = "python3 /home/cloud/htmlparser.py \"" + path + "\""
		println(str)
		s"python3 /home/cloud/htmlparser.py $path".!!
	}
	
	def detector(f: File) = {
		var cs: java.nio.charset.Charset = null
		try{
			cs = det.detectCodepage(f.toURI.toURL)
		}catch {
			case e: Throwable => log.error(e)
		}
//		log.info(cs.toString() + ":\t" + f.getName)
		cs.toString() match {
			case "UTF-8" => "UTF-8"
			case _ => "GBK"
		}
	}
	
	def folderToDocuments(x: File, db: MongoCollection[Document], fixdb: MongoCollection[Document] = null) = {
		assert(x.isDirectory, s"${x.getPath} is not a directory")
		var count = 0
		log.warn("new path start:" + x.getPath)		  		
		log.warn("GetAllFiles start: " + x.getPath)
		val files = getAllFiles(x)
		log.warn("GetAllFiles done: " + x.getPath)
		files.foreach(x => 
			try{
				var d = new Document
				val id = getwenshuID(x.getName)
				if (id != "")	d.append("_id", id)
				d.append("path", x.getPath)
				d.append("content", filterHtml(Source.fromFile(x, detector(x)).getLines().toArray).mkString("\n"))
				count = count+1
				try{
					db.insertOne(d)
					if (fixdb != null){
						fixdb.insertOne(ImportDataProcess.processData(d))
					}
				}catch {
					case e : Throwable => 
				}
				if (count == 10000) {
					log.warn("start insert 10000:")
					count = 0
					log.warn("finish insert 10000:")
				}
			}catch {
				case e: Throwable => log.error(e)
			})
		log.warn("All done: " + x.getPath)		
	}
	
  def main(args: Array[String]): Unit = {
  	val rootFolder = new File(rootPath)
  	val scheduler = Executors.newFixedThreadPool(8)
  	rootFolder.listFiles.foreach(y => {
  		val r = new Runnable(){
  			override def run(): Unit = {
  			y.listFiles().foreach(x => folderToDocuments(x, dbColl))
  			}
  		}
  		scheduler.execute(r)
  	})
  }
}