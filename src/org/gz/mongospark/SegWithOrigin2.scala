package org.gz
package mongospark

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import org.bson.Document
import org.gz.SegPage
import java.io.File
import scala.io.Source
import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark
import com.mongodb.client.model.Filters.regex
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._
import com.mongodb.client.model.Aggregates._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import scala.collection.JavaConverters._
import org.gz.util.Conf
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClientURI

/**
 * 二审分段数据的spark版本，
 */
object SegWithOrigin2 extends Conf{
	
	val segBase = ArrayBuffer[SegPage]()
	
	def getPri(str: String) = {
		str match {
			case "当事人" => 0 
			case "审理经过" => 1
			case "一审原告称"|"一审被告辩称"|"一审法院查明"|"一审法院认为"|"一审第三人称" => 2
			case "被上诉人辩称"|"上诉人诉称"|"第三人称"|"法庭辩论" => 2
			case "本院查明"|"本院认为" => 4
			case "裁判结果" => 5
			case "审判人员" => 6
			case "裁判日期" => 7
			case "书记员" => 8
			case "附" => 9
		}
	}
	
	def segByStart(reg: Array[Regex], str: String) = {
		var f = false
		reg.foreach(x => {
			val rs = x.findAllIn(str.trim)
  		if (rs.hasNext)
  			if (rs.start < 1)
  				f = true
  	})
  	f
	}
	
	def init = {
		val os = System.getProperty("os.name");  
		val homePath = if(os.toLowerCase().startsWith("win")) config.getString("path.windows") else config.getString("path.linux")   
		val path = Array("当事人", "审理经过",
				"一审被告辩称", "一审第三人称", "一审法院查明", "一审法院认为", "一审原告称",
				"上诉人诉称", "被上诉人辩称", "第三人称",
				"本院查明", "本院认为", "法庭辩论",
				"附", "审判人员", "裁判结果", "裁判日期", "书记员")
		for (f <- path){
			val pri = getPri(f) 
			val reg = Source.fromFile(new File(homePath + f)).getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*").replaceAll("==", "[^:：。.]*")}.mkString("|").r
			//println(reg)
			//val re = Source.fromFile(new File(homePath + f)).getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*").r }
			segBase += SegPage(f, Array(reg), pri)
		}
	}
	init
	
	def isyishen(str: String) = if ((str == "一审被告辩称")||(str == "一审第三人称")||(str == "一审法院查明")||(str == "一审法院认为")||(str == "一审原告称")) true else false
	
	def mergeyishen(processedData: ArrayBuffer[(String, String)]) = {
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
					yishenjingguo = processedData(i)._2 + yishenjingguo
				case _ => processedData(i - 1) = (processedData(i - 1)._1, processedData(i - 1)._2 + processedData(i)._2)
			}
		}
		一审经过.append("全文", yishenjingguo)
		一审.reverse.foreach(x => 一审经过.append(x._1, x._2))
		val arr全文 = ArrayBuffer[Document]()
		for (i <- 0 until firsty)
			if (processedData(i)._1 != "none"){ 
				d.append(processedData(i)._1, processedData(i)._2)
				val tmp = new Document 
				tmp.append(processedData(i)._1, processedData(i)._2)
				arr全文 += tmp
			}
		if (firsty > 0) {
			d.append("一审经过", 一审经过)
			val tmp = new Document 
			tmp.append("一审经过", 一审经过.get("全文"))
			arr全文 += tmp
		}
		for (i <- (lasty+1) until processedData.length) 
			if (processedData(i)._1 != "none"){
				d.append(processedData(i)._1, processedData(i)._2)
				val tmp = new Document 
				tmp.append(processedData(i)._1, processedData(i)._2)
				arr全文 += tmp
			}
		d.append("全文", arr全文.asJava)
	}
	
	def segment(arr: Array[String]) = {
	//结果
		val processedData = ArrayBuffer[(String, String)]()
		val hashset = HashSet[String]()
		var priority = 999
		var last = "none"
		var lastPriority = -1
		var tmp = "none"
		var strs = ""
		var blank = 0
		while (((arr(blank) == "")||(arr(blank) == "\r"))&&(blank < arr.length - 1)) blank = blank + 1
		val t = if (arr(blank).trim().endsWith("书")) blank + 1 else blank
		for (i <- t until arr.length){
			priority = 999
			tmp = ""
			segBase.foreach(x => {
				if (segByStart(x.reg, arr(i))&&(!hashset.contains(x.name)&&(getPri(x.name) >= lastPriority))){
					println(lastPriority)
					println(x.name + "\t" + arr(i))
					if ((x.name == last)&&(x.name != "当事人")) {
						//前面是当事人，后面能是别的就是别的，前面不是当事人，优先匹配相同字段
						tmp = last
						priority = x.priority
					} else if ((tmp == "一审法院查明")&&(x.name == "本院查明")){
						//如果出现本院查明意味着出现了二审本院等关键词，应该判断是本院查明
						priority = x.priority
						tmp = x.name						
					}else if ((priority > x.priority)&&(tmp != last)&&(tmp != "审理经过")){
						//在匹配出来的结果里选一个最小的，由于审理经过比当事人优先，所以将其设置为最优
						priority = x.priority
						tmp = x.name
					} else if ((x.name == "审理经过")&&(last == "当事人")){
						priority = x.priority
						tmp = x.name
					}					
				}
			})
			//采用规则：上一个不是裁判结果，并且以这些结尾，那么下面一段就是裁判结果
			//目测裁判结果都新开了一段，可以极大程度上避免一审判决分为裁判结果的错误
			if ((last != "裁判结果")&&(lastPriority < 6)&&(strs.endsWith("判决如下:")||strs.endsWith("判决如下：")||strs.endsWith("裁定如下：")||strs.endsWith("裁定如下:"))){
				processedData += ((last, strs))
				hashset += last
				last = "裁判结果"
				lastPriority = getPri("裁判结果")
				strs = ""
			}
			//采用规则：上一段是被上诉人辩称,上上段是一审，本段也是一审，那么将被上诉人辩称识别为一审被告辩称,由于下面规则存在，所以不会覆盖一审被告辩称字段导致冲突
			if (isyishen(tmp)&&(last == "被上诉人辩称"))
				if (isyishen(processedData(processedData.length-1)._1))
					last = "一审被告辩称"
			//采用规则：一审被告辩称后面不会出现被上诉人辩称，更加可能出现的是其他被告辩称，或者一审其他流程或者上诉人诉称
			if ((last == "一审被告辩称")&&(tmp == "被上诉人辩称")) tmp = "一审被告辩称"
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
		val d = mergeyishen(processedData)
		d
	}
	
	lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo.getDatabase("wenshu")
	private lazy val dbColl = db.getCollection("origin2")
	
	def main(args: Array[String]): Unit = {
 // 	System.setProperty("hadoop.home.dir", "D:/hadoop-common")
    val spark = new MongoUserUtils().sparkSessionBuilder()
   	val rdd = MongoSpark.builder().sparkSession(spark).pipeline(Seq(`match`(eqq("basiclabel.procedure", "二审")))).build.toRDD()   	
   	rdd.cache()
   	println(rdd.count())
   	val c = 11.toChar
   	rdd.foreach{ x => {
   		try{
   			val str = x.getString("content").split(s"[${c}\n]")
   			dbColl.updateOne(eqq("_id", x.get("_id")), set("segdata", segment(str)))
   		}catch{
   			case e: Throwable => e.printStackTrace()
   		}
   	}}
    mongo.close()
	}
}