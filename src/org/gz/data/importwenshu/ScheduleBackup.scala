package org.gz.data.importwenshu

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.mongodb.spark.MongoSpark
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.regex
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Filters.or
import com.mongodb.client.model.Updates._
import com.mongodb.client.model.Aggregates._
import java.text.SimpleDateFormat
import org.apache.logging.log4j.LogManager
import com.mongodb.client.model.UpdateOptions
import com.typesafe.config.ConfigFactory
import org.gz.util.Conf
import org.apache.spark.storage.StorageLevel
import java.util.ArrayList
import org.bson.Document
import com.mongodb.InsertOptions
import com.mongodb.client.model.InsertManyOptions
import scala.collection.JavaConverters._
import org.gz.util.MongoUserUtils
import scala.collection.mutable.ArrayBuffer

object ScheduleBackup extends Conf{
	// 	System.setProperty("hadoop.home.dir", "D:/hadoop-common")
	val log = LogManager.getLogger(this.getClass.getName())
	val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))	
	lazy val spark = new MongoUserUtils().sparkSessionBuilder() 
	
	//带认证的方式调用MongoDB会出现不能初始化的错误，我觉得是因为mongoURI不能序列化的原因。
//	val mongoURI = new MongoClientURI(s"")
//	val mongo = new MongoClient(mongoURI)
//	val db = mongo.getDatabase("wenshu")
//	val dbColl = db.getCollection("origin")
	
	val sdf = new SimpleDateFormat("yyyyMMdd")	
	
  def doBackUp(c: Calendar) = {
		val backName = s"backup${sdf.format(c.getTime)}"
		c.add(Calendar.DAY_OF_MONTH, -21)
		val muu = new MongoUserUtils
		try{
			val mongoURI2 = new MongoClientURI(muu.backupMongoURI)
			val mongo2 = new MongoClient(mongoURI2)
			val db2 = mongo2.getDatabase("wenshu")
			val dbColl3 = db2.getCollection(s"backup${sdf.format(c.getTime)}")
			dbColl3.drop
			mongo2.close
		}catch{
			case e: Throwable => log.error("drop3周前的表失败") 
		}
  	val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()
  	rdd.persist(StorageLevel.MEMORY_AND_DISK)
   	println(rdd.count())   	
   	val uri = muu.clusterMongoURI
   	val uri2 = muu.backupMongoURI
  	rdd.foreachPartition { x => {  		
  		val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("wenshu")
			val dbColl = db.getCollection("originbackup")
						
			val mongoURI2 = new MongoClientURI(uri2)
			val mongo2 = new MongoClient(mongoURI2)
			val db2 = mongo2.getDatabase("wenshu")
			val dbColl2 = db2.getCollection(backName)
			
			var count = 0
			var resList = new ArrayList[Document]
			x.foreach(y => {
//				count = count + 1
//				resList add y
				try{
					dbColl.replaceOne(eqq("_id", y.get("_id")), y, new UpdateOptions().upsert(true))
					dbColl2.insertOne(y)
				}catch{
					case e: Throwable => e.printStackTrace()
				}
				
//				使用这种方式插入会导致插入的数据和真实数据数量对应不上， 先注释掉有机会再找原因
//				if (count == 10000){
//					try{					
//						dbColl2.insertMany(resList, new InsertManyOptions().ordered(false))
//					}catch{
//						case e: Throwable => e.printStackTrace()
//					}
//					resList.clear
//					count = 0
//				}
			})
//			if (count > 0)
//				try{					
//					dbColl2.insertMany(resList, new InsertManyOptions().ordered(false))
//				}catch{
//					case e: Throwable => e.printStackTrace()
//				}
  		mongo.close
  		mongo2.close
  	} }
  }
	
	def extractCasecausesToNewTable(name: String, caseCauses: Array[String]) = {
		assert((name != ""), "name cant be ''")
		assert((caseCauses.length > 0), "caseCauses cant be null")
		val muu = new MongoUserUtils
		val caseBson = caseCauses.map{x => eqq("basiclabel.casecause", x)}
		val rdd = MongoSpark.builder().sparkSession(spark).pipeline(Seq(`match`(or(caseBson: _*)))).build().toRDD()
  	rdd.persist(StorageLevel.MEMORY_AND_DISK)
   	println(rdd.count())   	
   	val uri = muu.clusterMongoURI
  	rdd.foreachPartition { x => {  		
  		val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("datamining")
			val dbColl = db.getCollection(name)
			x.foreach(y => {
				try{
					dbColl.insertOne(y)
				}catch{
					case e: Throwable => e.printStackTrace()
				}
			})
  		mongo.close
  	} }
	}
	
	def mergeTableToOrigin(name: String) = {
		
	}
	
	def main(args: Array[String]): Unit = {
		args.length match {
			case 0 => 
				println("Usage: Array[String], this is a program use to extract data from wenshu.orgin2 to datamining.collName or merge from datamining.collName to wenshu.origin2")
				println("args 0: the method, extract or merge")
				println("args 1: name, the collection name which want to 'extract to' or 'merge from'")
				println("args 2-n: if you want to extract, please input the casecauses")
			case 1 => 
				println("error, we need ad least 2 arguments!")
				println("Usage: Array[String], this is a program use to extract data from wenshu.orgin2 to datamining.collName or merge from datamining.collName to wenshu.origin2")
				println("args 0: the method, extract or merge")
				println("args 1: name, the collection name which want to 'extract to' or 'merge from'")
				println("args 2-n: if you want to extract, please input the casecauses")
			case _ =>
				if (args(0).toLowerCase() == "extract"){
					var casecauses = new Array[String](args.length - 2)
					Array.copy(args, 2, casecauses, 0, args.length - 2)
					println("casecauses: " + casecauses.mkString(", "))
					extractCasecausesToNewTable(args(1), casecauses)
				} 
				else if (args(0).toLowerCase() == "merge"){
					mergeTableToOrigin(args(1))
				}
		}
	}
}