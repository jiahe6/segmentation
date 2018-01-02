package org.gz

import org.gz.util.Conf
import org.gz.util.MongoUserUtils
import com.mongodb.spark.MongoSpark
import java.io.PrintWriter
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer
import java.io.File
import org.apache.spark.storage.StorageLevel
import org.bson.Document
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.FileInputStream
import scala.collection.JavaConversions._
import org.apache.poi.ss.usermodel.Row
import java.util.ArrayList
import scala.collection.mutable.HashMap
import scala.io.Source
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import scala.util.Try
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates.set
import org.gz.util.Utils
import tp.file.label.FindLabelByMongo
import tp.mining.law.FindLawByMongo

object SeperateCollection extends Conf{

	def sparkSeperate = {
		val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))	
		lazy val spark = new MongoUserUtils().sparkSessionBuilder(jarName = "SeperateCollection.jar")
		val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()
		rdd.persist(StorageLevel.MEMORY_AND_DISK)
		println(rdd.count())
		val r2 = rdd.map{ x =>
			val basicl = x.get("basiclable", classOf[Document])			
			if (basicl != null) {
				val str = basicl.getString("casecause")
				(str, 1)
			}else (null,1)
		}.reduceByKey(_+_).collect()
		val writer = new PrintWriter(new File("C:/Users/cloud/Desktop/类案搜索/数据统计/数据量统计"))
		println("数据量统计完成\n!\n!\n!")	
		implicit val keyOrdering = new Ordering[(String, Int)]{
			override def compare(x: (String, Int), y : (String, Int)) = {
				x._2.compare(y._2)
			}
		}		
		val sortedRes = r2.sorted(keyOrdering)
		sortedRes.foreach(x => {
			writer.write(x._1 + "->" + x._2 + "\n")
			println(x._1 + "->" + x._2)
		})
		writer.close()
		var count = 0
		var t = 0
		var arrlist = ArrayBuffer[String]()
		for (i <- 0 until sortedRes.length){
			count = count + sortedRes(i)._2
			arrlist += sortedRes(i)._1
			if (count > 2000000){
				count = 0
				val writer2 = new PrintWriter(new File("C:/Users/cloud/Desktop/类案搜索/数据统计/" + t))
				t = t + 1				
				writer2.write(count + "\n")
				writer2.write(arrlist.mkString(",") + "\n")
				writer2.close()
				arrlist.clear()
			}
		}
		if (count != 0) {
			count = 0
			val writer2 = new PrintWriter(new File("C:/Users/cloud/Desktop/类案搜索/数据统计/" + t))
			t = t + 1				
			writer2.write(count + "\n")
			writer2.write(arrlist.mkString(",") + "\n")
			writer2.close()
			arrlist.clear()
		}
	}
	
	def getContent(rows: Iterator[Row]) = {
		val ab = ArrayBuffer[(String, String, Int)]()
		rows.foreach { x =>
			ab += ((x.getCell(0).toString(), x.getCell(1).toString(), x.getCell(2).toString().toDouble.toInt))
		}
		ab
	}
	
	def countError = {
		val workbook = new XSSFWorkbook(new FileInputStream("C:/Users/cloud/Desktop/类案搜索/数据分析杨天泰和数据组资料/民间借贷纠纷金融借款纠纷内部测试案情特征.xlsx"))
		val caseCauseMap = HashMap(
				"金融" -> "金融借款合同纠纷",
				"民间借贷" -> "民间借贷纠纷"
				)
		val sheet = workbook.getSheet("说明")
		val rows = sheet.rowIterator()
		rows.foreach { x => ??? }
	}
	
	def seperate = {
		val workbook = new XSSFWorkbook(new FileInputStream("C:/Users/cloud/Desktop/类案搜索/数据分析杨天泰和数据组资料/分案件类型的案由案件数量统计.xlsx"))
		val map = Array("刑事", "民事", "行政", "赔偿")
		map.foreach { x =>
			val sheet = workbook.getSheet(x)
			val rows = sheet.rowIterator().drop(1)
			val arr = getContent(rows).sortBy(-_._3)	
			val threshold = 2000000
	    val group = ArrayBuffer[(ArrayList[String], Int)]()
	    val flag = new Array[Boolean](arr.length)
	    flag.map(x => false)
	    var count = 0
	    while (count < arr.length){
	    	val arrList = new ArrayList[String]
	    	var totalDataNum = 0
	    	for (i <- 0 until arr.length)
	    		if (!flag(i))
		    		if ((arr(i)._3 > threshold)&&(totalDataNum == 0)) {
		    			//由于sorted过了，所以无所谓
		    			arrList += arr(i)._1	    			
		    			count = count + 1
		    			totalDataNum = arr(i)._3		    			
		    			flag(i) = true
		    		} else if ((arr(i)._3 + totalDataNum) < threshold){
		    			arrList += arr(i)._1	    			
		    			count = count + 1
		    			totalDataNum = totalDataNum + arr(i)._3		    			
		    			flag(i) = true
		    		}	    		
		    group += ((arrList, totalDataNum))
	    }
			val rootPath = "C:/Users/cloud/Desktop/类案搜索/数据拆分分组/"
			group.foreach(x => { 				
				println(x._2)
				println(x._1.length)
				val writer = new PrintWriter(new File(rootPath + x._1(0)))
				println(x._1.mkString(","))
				writer.write(x._1.mkString(","))
				writer.close()
			})
		}

	}
	
	def doSeperateData = {
		val rootfile = new File("C:/Users/cloud/Desktop/类案搜索/数据拆分分组/")
		//casecause -> CollectonName
		val hm = new HashMap[String, String]()
		rootfile.listFiles().foreach { x =>
			Source.fromFile(x).getLines().foreach { y =>
				val arr = y.split(",")
				arr.foreach { z => hm += ((z, x.getName)) }
			}
		}
		hm.foreach(println)
		val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))
		val muu = new MongoUserUtils
		lazy val spark = muu.sparkSessionBuilder(jarName = "SeperateCollection.jar")
		val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()
		rdd.persist(StorageLevel.MEMORY_AND_DISK)
		println(rdd.count())
		val mongosUris = config.getString("mongodb.clusteruriall")
		rdd.foreachPartition{ iter =>
			//选取最近的mongos进行数据更新
			val muuLocal = new MongoUserUtils
			val uri = s"${Utils.getIpAddress}:27017"
			println(s"local uri is: ${uri}")
			val mongoURI = 
				if (mongosUris.split(",").contains(uri))
			 		new MongoClientURI(muuLocal.clusterLocalMongoURI(uri))
				else 
					new MongoClientURI(muuLocal.clusterURI)
			println(s"local mongouri is: ${mongoURI.getURI}")
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("datamining")			
			iter.foreach{ x => 
				val basicl = x.get("basiclabel", classOf[Document])			
				if (basicl != null) {
					val str = basicl.getString("casecause")
					val collName = hm.get(str) 
					collName match {
						case Some(s) =>
							Try{
								db.getCollection(s).insertOne(x)								
							}
						case None =>
					}						
				}}
			mongo.close()
		}
		
	}
	
	def doInsertMD5 = {
		val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))
		val muu = new MongoUserUtils
		lazy val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI("datamining.testsparkIO"), jarName = "SeperateCollection.jar")
		val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()
		rdd.persist(StorageLevel.MEMORY_AND_DISK)
		println(rdd.count())		
		val uri = muu.clusterMongoURI
		rdd.foreachPartition{ iter =>
			val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("datamining")
			val dbColl = db.getCollection("testsparkIO")
			val md = java.security.MessageDigest.getInstance("MD5")
			iter.foreach{ x => {				
				val md5 = md.digest(x.getString("content").getBytes).map("%02x".format(_)).mkString
				val id = x.getString("_id")
				dbColl.updateOne(eqq("_id", id), set("md5", md5))
			}}
			mongo.close()
		}
	}
	
		def doUpdateBasicLabel = {
		val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))
		val muu = new MongoUserUtils
		lazy val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI("datamining.公司决议效力确认纠纷"), jarName = "yuanyuanspark.jar")
		val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()	
		val uri = muu.clusterMongoURI
		rdd.foreachPartition{ iter =>
			val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("datamining")
			val dbColl = db.getCollection("公司决议效力确认纠纷")
			iter.foreach{ x => {				
				val id = x.getString("_id")
				val doc=FindLabelByMongo.backups1227(x)
				println(x.getString("casetype2"))
				println("---------------------------")
				dbColl.updateOne(eqq("_id", id), set("basiclabel",doc))
			}}
			mongo.close()
		}
	}
		
				def doFindLaw = {
		val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))
		val muu = new MongoUserUtils
		lazy val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI("datamining.test_yy"), jarName = "yuanyuansparkLaw.jar")
		val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()	
		val uri = muu.clusterMongoURI
		rdd.foreachPartition{ iter =>
			val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("datamining")
			val dbColl = db.getCollection("test_yy")
			iter.foreach{ x => {
				val id = x.getString("_id")
				val doc=FindLawByMongo.readLaw(x)
				println("---------------------------")
				dbColl.updateOne(eqq("_id", id), set("mininglabel",doc))
			}}
			mongo.close()
		}
	}
	
	def main(args: Array[String]): Unit = {
//	  val muu = new MongoUserUtils
//	  	val uri = muu.clusterMongoURI
//	  val mongoURI = new MongoClientURI(uri)
//			val mongo = new MongoClient(mongoURI)
//			val db = mongo.getDatabase("datamining")
//			val dbColl = db.getCollection("test_yy")
//			
//			val iter = dbColl.find().iterator()
//			iter.foreach(x => {
//			  val doc=FindLabelByMongo.backups1227(x)
//			  println(doc.getString("casetype2"))
//			  println("--------------------------")
//			})
//			mongo.close()
//	  doUpdateBasicLabel
	  doFindLaw
//		doSeperateData
		//doInsertMD5
	}
}