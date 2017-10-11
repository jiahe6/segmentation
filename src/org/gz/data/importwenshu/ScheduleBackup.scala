package org.gz.data.importwenshu

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.mongodb.spark.MongoSpark
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient

object ScheduleBackup {
	// 	System.setProperty("hadoop.home.dir", "D:/hadoop-common")
	lazy val spark = SparkSession.builder()
		.master("spark://192.168.12.161:7077")
		.config(new SparkConf().setJars(Array("hdfs://192.168.12.161:9000/mongolib/mongo-spark-connector_2.11-2.0.0.jar",
				"hdfs://192.168.12.161:9000/mongolib/bson-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/mongo-java-driver-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/mongodb-driver-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/mongodb-driver-core-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/commons-io-2.5.jar",
				"hdfs://192.168.12.161:9000/segwithorigin2.jar")))  	  
		.config("spark.cores.max", 80)		
		.config("spark.executor.cores", 16)
		.config("spark.executor.memory", "32g")
		.config("spark.mongodb.input.uri", "mongodb://gaoze:qazwsxedc!@192.168.12.161:27017/wenshu.origin2?authSource=admin")
		.getOrCreate()
	
	private lazy val mongoURI = new MongoClientURI("mongodb://gaoze:qazwsxedc!@192.168.12.161:27017/?authSource=admin")
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo.getDatabase("wenshu")
	private lazy val dbColl = db.getCollection("origin")
	
	private lazy val mongoURI2 = new MongoClientURI("mongodb://gaoze:qazwsxedc!@192.168.12.:27017/?authSource=admin")
	private lazy val mongo2 = new MongoClient(mongoURI)
	private lazy val db2 = mongo2.getDatabase("wenshu")
	private lazy val dbColl2 = db2.getCollection("origin")
	
  def doBackUp(c: Calendar) = {
  	val rdd = MongoSpark.builder().sparkSession(spark).build.toRDD()
  	rdd.cache()
   	println(rdd.count())
   	rdd.foreach{x => 
   		dbColl.insertOne(x)
   	}
  }
}