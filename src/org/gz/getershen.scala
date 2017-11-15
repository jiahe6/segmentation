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
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.BasicDBObject
import com.mongodb.client.model.Filters.{regex => regexx}
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._
import com.mongodb.client.model.Aggregates._
import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils


/**
 * @author cloud
 *	二审分段程序，也进行全量数据导入工作
 * 分段程序入口都走SegWithOrigin2.fenduan，那个比较新
 */
object getershen {
	
	private val rootPath = "D:/library/wenshu/20150730"
	private val ls = "D:/library/wenshu/20131231/最高人民法院/0/民事案件/（2013）民二终字第64号_f05d5807-b647-11e3-84e9-5cf3fc0c2c18裁定书.txt"
	private val log = LogManager.getLogger(this.getClass.getName())
	private lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo.getDatabase("wenshu")
	private lazy val dbColl = db.getCollection("origin2")

	private val files = ArrayBuffer[File]()
	private val regex = "终(字|第)".r
	//结果
	private val processedData = ArrayBuffer[(String, String)]()
	//分段依据
	private val segBase = HashSet[SegPage]()
	//探测器
	private val det = CodepageDetectorProxy.getInstance()
	//hashSet
	private val hashset = HashSet[String]()
	det.add(new ParsingDetector(false))
	det.add(JChardetFacade.getInstance)
	det.add(ASCIIDetector.getInstance)
	det.add(UnicodeDetector.getInstance)
	
	def insertMany(kvs: java.util.List[java.util.Map[String, Any]]) = {
		kvs.foreach(insertOnce)
	}
	
	def insertOnce(kv: java.util.Map[String, Any]) = {
		var doc = new Document
		kv.foreach(x => doc.append(x._1, x._2))
		dbColl.insertOne(doc)
	}
	
	def getPri(str: String) = {
		str match {
			case "当事人" => 0 
			case "审理经过" => 1
			case "一审原告称"|"一审被告辩称"|"一审法院查明"|"一审法院认为"|"被上诉人辩称"|"上诉人诉称"|"一审第三人称"|"第三人称" => 2
			case "本院查明"|"本院认为"|"法庭辩论" => 3
			case "裁判结果" => 4
			case "审判人员" => 5
			case "裁判日期" => 6
			case "书记员" => 7
			case "附" => 8
		}
	}
	
	def init = {
		val path = new File("关键词/")
		for (file <- path.listFiles()){
			val pri = getPri(file.getName) 
			val reg = Source.fromFile(file.getPath).getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*")}.mkString("|").r
			//println(reg)
			//val re = Source.fromFile(file.getPath).getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*")r }
			segBase += SegPage(file.getName, Array(reg), pri)
		}
	}
	init
	
	def getFiles(path: File): Unit = {
		val folders = new ArrayBuffer[File]
		folders += path
		var count = 0
		while (count < folders.length){ 
			if (folders(count).isFile()&&(folders(count).getName.endsWith("txt"))&&(regex.findAllIn(folders(count).getName).hasNext)) files += folders(count) else 
				if (folders(count).isDirectory()) folders(count).listFiles().foreach(x => {
					if (x.isFile()&&(x.getName.endsWith("txt"))&&(regex.findAllIn(x.getName).hasNext)) files += x else
						if (x.isDirectory()) folders += x
				})
			count = count + 1	
		}
	}
	
	def getAllFiles(path: File): Unit = {
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
	
	@deprecated
	def fenduan(arr: Array[String], fileName: String, outFile: File = null) = {
		processedData.clear()
		hashset.clear()
		var priority = 999
		var last = "none"
		var lastPriority = -1
		var tmp = "none"
		var strs = ""
		for (i <- 0 until arr.length){
			priority = 999
			tmp = ""
			segBase.foreach(x => {
				if (segByStart(x.reg, arr(i))&&(!hashset.contains(x.name)&&(getPri(x.name) >= lastPriority))){
//					println(x.name + "\t" + arr(i))
					if ((x.name == last)&&(x.name != "当事人")) tmp = last
					if ((priority >= x.priority)&&(tmp != last)&&(tmp != "审理经过")){
						priority = x.priority
						tmp = x.name
					} else if ((x.name == "审理经过")&&(last == "当事人")){
						priority = x.priority
						tmp = x.name
					}					
				}
			})
			if ((last != "裁判结果")&&(strs.endsWith("判决如下:")||strs.endsWith("判决如下：")||strs.endsWith("裁定如下：")||strs.endsWith("裁定如下:"))){
				processedData += ((last, strs))
				hashset += last
				last = "裁判结果"
				lastPriority = getPri("裁判结果")
				strs = ""
			}
			if ((priority >= lastPriority)&&(tmp != last)&&(priority != 999)){
				processedData += ((last, strs))
				hashset += last
				last = tmp 
				lastPriority = priority
				strs = ""
			}
			strs = strs + "\n" + arr(i)
			
		}
		processedData += ((last, strs))	
		if (outFile == null){			
		} else {
	  	val writer = new PrintWriter(outFile)
			processedData.foreach(x => writer.write(x.toString() + "\n"))
    	writer.close()
    }
		var d = new Document
		var 一审经过 = new Document
		var yishenjingguo = ""
		var firsty = 0
		var lasty = -1
		for (i <- 0 until processedData.length){
			processedData(i)._1 match{
				case "上诉人诉称" => processedData(i) = ("诉称", processedData(i)._2)
				case "被上诉人辩称" => processedData(i) = ("辩称", processedData(i)._2)
				case "一审被告辩称"|"一审第三人称"|"一审法院查明"|"一审法院认为"|"一审原告称" =>
					if (firsty == 0) firsty = i
					lasty = i					
				case _ => 
			}
		}
		var 一审 = ArrayBuffer[(String, String)]()
		for (i <- (firsty to lasty).reverse){
			processedData(i)._1 match{
				case "一审被告辩称"|"一审第三人称"|"一审法院查明"|"一审法院认为"|"一审原告称" => 
					一审 += ((processedData(i)._1, processedData(i)._2))
					yishenjingguo = processedData(i)._2 + "\n" + yishenjingguo
				case _ => processedData(i - 1) = (processedData(i - 1)._1, processedData(i - 1)._2 + processedData(i)._2)
			}
		}
		一审经过.append("全文", yishenjingguo)
		一审.reverse.foreach(x => 一审经过.append(x._1, x._2))
		for (i <- 0 until firsty)
			if (processedData(i)._1 != "none") 
				d.append(processedData(i)._1, processedData(i)._2)
		if (firsty > 0) d.append("一审经过", 一审经过)
		for (i <- (lasty+1) until processedData.length) 
			if (processedData(i)._1 != "none")
				d.append(processedData(i)._1, processedData(i)._2)
		d
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
//		cs.toString()
		cs.toString() match {
			case "UTF-8" => "UTF-8"
			case _ => "GBK"
		}
	}
	
	def ceshidaima = {
//  	val arr = Source.fromFile(ls, "GBK").getLines().toArray
//  	fenduan(filterHtml(Source.fromFile(ls, "GBK").getLines().toArray), new File("result/（2017）京02民终223号_faf3080f-fcfa-455f-9bc1-a762000dc664判决书.txt"))
  	val str = "原审判决认定事实，2014年10月3日22时许，被告人车云翩窜至曲靖市麒麟区三江大道“AP女星密码”店内，将被害人黄云翩放于店内的现金人民币1328元盗走。"
//  	val re = Source.fromFile("关键词/一审法院认为").getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*")r }  	
//  	re.foreach { x => println(x +":"+ x.findFirstIn(str)) }
  	val reg = "原审判决认定[^(事实)]".r
  	println(reg.findFirstIn(str))
//  	val s = HashMap[String,String]()
//  	files.foreach(x => s += ((detector(x), x.getName)))
//  	s.foreach(println)		
	}
	
  def main(args: Array[String]): Unit = {

  	//抽二审的方法
//  	val rootFolder = new File(rootPath)
//  	rootFolder.listFiles.foreach(x => {
//  		files.clear()
//  		getFiles(x)
// 			files.foreach(x => 
//	 			try{
// 					fenduan(filterHtml(Source.fromFile(x, detector(x)).getLines().toArray), x.getName, new File("result/" + x.getName))
// 				}catch {
//	 				case e: Throwable => log.error(e)
// 				})
// 		})

// 		val dbo = new BasicDBObject()
// 		println(regex.toString())
//  	dbo.put("path", java.util.regex.Pattern.compile(regex.toString()))
//  	val docs = dbColl.find(dbo)
 		val docs = dbColl.find(eqq("basiclabel.procedure", "二审")).iterator()
 		log.info("start")
 		var count = 0
 		while (docs.hasNext()){
 			count = count+1
 			val x = docs.next()
 			try{
   			val str = x.getString("content").split("\n")
   			val name = x.getString("path")
   			dbColl.updateOne(eqq("_id", x.get("_id")), set("segdata", fenduan(str, name)))
   		}catch{
   			case e: Throwable => log.error(e + "\n" + e.getStackTrace.mkString("\n"))
   		}
   		if (count % 100 == 0) log.info("处理了:" + count)
 		}
//  	docs.foreach { x => {
//   		try{
//   			val str = x.getString("content").split("\n")
//   			val name = x.getString("path")
//   			dbColl.updateOne(eqq("_id", x.get("_id")), set("segdata", fenduan(str, name)))
//   		}catch{
//   			case e: Throwable => log.error(e + "\n" + e.getStackTrace.mkString("\n"))
//   		}
//   	}}
  	
  	
  	//抽原始文书的方法
/*  	var resList = new ArrayList[Document]
  	var count = 0
  	val rootFolder = new File(rootPath)
  	rootFolder.listFiles.foreach(y => y.listFiles().foreach(x => {
  		log.warn("new path start:" + x.getPath)
  		files.clear()
  		log.warn("GetAllFiles start: " + x.getPath)
  		getAllFiles(x)
  		log.warn("GetAllFiles done: " + x.getPath)
 			files.foreach(x => 
	 			try{
	 				log.info("start wenshu chuli:" + x.getPath)
 					var d = new Document
					val id = getwenshuID(x.getName)
					if (id != "")	d.append("_id", id)
					d.append("path", x.getPath)
					d.append("wenshuName", x.getName)
					d.append("content", filterHtml(Source.fromFile(x, detector(x)).getLines().toArray).mkString("\n"))
					resList.add(d)
					count = count+1
					log.info("end wenshu chuli:" + x.getPath)
					if (count >= 10000) {
						log.warn("start insert 10000: " + count)
						try{
							dbColl.insertMany(resList, new InsertManyOptions().ordered(false))
						}catch {
							case e: Throwable => log.error(e)
						}finally{
							count = 0
							resList.clear
							log.warn("finish insert 10000, count = :" + count)
						}
					}
 				}catch {
	 				case e: Throwable => log.error(e)
 				})
 			log.warn("All done: " + x.getPath)
 		})
  	)
  	try{
 			dbColl.insertMany(resList, new InsertManyOptions().ordered(false))
  	}catch {
	 		case e: Throwable => log.error(e)
 		}*/
  }
}