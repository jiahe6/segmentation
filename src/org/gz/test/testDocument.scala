package org.gz.test

import com.mongodb.MongoClient
import org.bson.Document
import scala.io.Source
import com.mongodb.spark.config.ReadConfig
import scala.reflect.ClassTag
import org.bson.conversions.Bson
import org.gz.mongospark.SegWithOrigin2
import java.io.File
import scala.collection.mutable.ArrayBuffer

object testDocument {
	
	def findRegex(file: String, str: String) = {
		val arr = Source.fromFile(new File("关键词/" + file)).getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*").replaceAll("==", "[^:：。.]*").r}
		arr.foreach(x => {			
			val it = x.findAllIn(str.trim)
  		if (it.hasNext)
  			if (it.start < 1)
  				println(x.toString() + "\t:\t" + it.next())
		})
	}
	
  def main(args: Array[String]): Unit = {
  	lazy val mongo = new MongoClient("192.168.12.161", 27017)
		lazy val db = mongo.getDatabase("wenshu")
		lazy val dbColl = db.getCollection("sample_gz")
		val d = new Document
//		d.append("_id", "123asd")
//		d.append("测试", "123asd")
//		d.append("测试", "234sdf")
//		d.append("测试", "345dfg")
//		println(d.get("_id"))
//		dbColl.insertOne(d)
//  	val path = Array("被上诉人辩称", "本院查明", "本院认为", "裁判结果", "裁判日期", "当事人", "第三人称", "法庭辩论", "附", "上诉人诉称", "审理经过", "审判人员",
//				"书记员", "一审被告辩称", "一审第三人称", "一审法院查明", "一审法院认为", "一审原告称")
//		for (f <- path){
//			println(f)
//  		Source.fromInputStream(ClassLoader.getSystemClassLoader().getResourceAsStream("resources/" + f)).getLines().foreach(println)
//		}
  	val regex = "[^:：。.]*(原审|一审)[^:：,。.，]*[^上]诉称|[^:：。.]*原审诉称".replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*").replaceAll("==", "[^:：。.]*").r
//  	val r = "[^:：]*二审期间".r
  	val it = regex.findAllIn("刘清芳原审六六六上诉称：刘清芳到达法定退休年龄后")
  	if (it.hasNext){
  		println(it.next())
  		println(it.start)
  	}
  	import com.mongodb.client.model.Filters.{eq => eqq}
  	val docs = dbColl.find(eqq("_id", "0e927fe8-f1b1-4b01-a007-8006ff3a8ece")).iterator()
  	val str1 = docs.next().getString("content")
//  	val c = 11.toChar
//  	val arrr = Array(dddd.getString("content")).flatMap{ x => x.split(s"[${c}\n]") }
//  	println(arrr.size)
//  	arrr.foreach { x => {println(x + "\n")} }
//  	println(11.toChar)
  	
		val str4 = """双方当事人在二审均未向法庭提供新的证据
"""
		findRegex("上诉人诉称", """123上诉人王晓培、贾维杰辩称，一审判决认定事实清楚，适用法律正确，请求依法驳回上诉，维持原判。
		  """)
		val c = 11.toChar
		val docu = SegWithOrigin2.segment(str4.split(s"[${c}\n]"))
		import scala.collection.JavaConversions._
    val arr = docu.get("全文", classOf[java.util.List[Document]])
   	arr.foreach{ println }
		//println(docu)
//		import scala.collection.JavaConverters._
//		val readConfig = ReadConfig("wenshu", "origin2", connectionString = Some("mongodb://192.168.12.161:27017"))
//  	mongo.getDatabase("")
//  	.getCollection[Document](readConfig.collectionName, classOf[Document])
//  	.withReadConcern(readConfig.readConcern)
//  	.withReadPreference(readConfig.readPreference)
//  	.aggregate(List[Bson]().asJava)
// // 	.allowDiskUse(true).
 // 	.iterator()
  }
}