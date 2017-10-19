package org.gz.data.importwenshu

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.mongodb.spark.MongoSpark
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.regex
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._
import com.mongodb.client.model.Aggregates._
import java.text.SimpleDateFormat
import org.apache.logging.log4j.LogManager
import com.mongodb.client.model.UpdateOptions
import com.typesafe.config.ConfigFactory
import org.gz.util.Conf

object ScheduleBackup extends Conf{
	// 	System.setProperty("hadoop.home.dir", "D:/hadoop-common")
	val log = LogManager.getLogger(this.getClass.getName())
	val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))	
	lazy val spark = SparkSession.builder()
		.master("spark://192.168.12.161:7077")
		.config(new SparkConf().setJars(Array("hdfs://192.168.12.161:9000/mongolib/mongo-spark-connector_2.11-2.0.0.jar",
				"hdfs://192.168.12.161:9000/mongolib/bson-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/mongo-java-driver-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/mongodb-driver-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/mongodb-driver-core-3.4.2.jar",
				"hdfs://192.168.12.161:9000/mongolib/commons-io-2.5.jar",
				"hdfs://192.168.12.161:9000/mongolib/config-1.2.1.jar",
				"hdfs://192.168.12.161:9000/ScheduleImport.jar")))  	  
		.config("spark.cores.max", 80)		
		.config("spark.executor.cores", 16)
		.config("spark.executor.memory", "32g")
		.config("spark.mongodb.input.uri", s"mongodb://${user}:${passwd}@192.168.12.161:27017/wenshu.casetype_yy?authSource=${authDB}")
		.config("spark.mongodb.output.uri", s"mongodb://${user}:${passwd}@192.168.12.160:27017/wenshu.backup?authSource=${authDB}")
		.config("spark.mongodb.input.partitionerOptions.samplesPerPartition", 1)
		.getOrCreate()
	
	//带认证的方式调用MongoDB会出现不能初始化的错误，我觉得是因为mongoURI不能序列化的原因。
//	val mongoURI = new MongoClientURI(s"mongodb://@192.168.12.161:27017/?authSource=admin")
//	val mongo = new MongoClient(mongoURI)
//	val db = mongo.getDatabase("wenshu")
//	val dbColl = db.getCollection("origin")
	
	val sdf = new SimpleDateFormat("yyyyMMdd")	
	
  def doBackUp(c: Calendar) = {
		val backName = s"backup${sdf.format(c.getTime)}"
		c.add(Calendar.DAY_OF_MONTH, -21)
		try{
			val mongoURI2 = new MongoClientURI(s"mongodb://${config.getString("mongo.backup.user")}:${config.getString("mongo.backup.user")}@192.168.12.160:27017/?authSource=${config.getString("mongo.backup.user")}")
			val mongo2 = new MongoClient(mongoURI2)
			val db2 = mongo2.getDatabase("wenshu")
			val dbColl3 = db2.getCollection(s"backup${sdf.format(c.getTime)}")
			dbColl3.drop
			mongo2.close
		}catch{
			case e: Throwable => log.error("drop3周前的表失败") 
		}
  	val rdd = MongoSpark.builder().sparkSession(spark).build().toRDD()
//  	val df = MongoSpark.builder().sparkSession(spark).build.toDF
//  	MongoSpark.save(df.write)
  	rdd.cache()
   	println(rdd.count())  
   	val uri = s"mongodb://${user}:${passwd}@192.168.12.161:27017/?authSource=${authDB}"
   	val uri2 = s"mongodb://${config.getString("mongo.backup.user")}:${config.getString("mongo.backup.passwd")}@192.168.12.160:27017/?authSource=${config.getString("mongo.backup.authDB")}"
  	rdd.foreachPartition { x => {  		
  		val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("wenshu")
			val dbColl = db.getCollection("origin")
			
			//val mongoURI2 = new MongoClientURI(s"mongodb://${config.getString("mongo.backup.user")}:${config.getString("mongo.backup.user")}@192.168.12.160:27017/?authSource=${config.getString("mongo.backup.user")}")
			val mongoURI2 = new MongoClientURI(uri2)
			val mongo2 = new MongoClient(mongoURI2)
			val db2 = mongo2.getDatabase("wenshu")
			val dbColl2 = db2.getCollection(backName)

			x.foreach { y => 
				dbColl.replaceOne(eqq("_id", y.get("_id")), y, new UpdateOptions().upsert(true))
				dbColl2.insertOne(y)
  		}
  		mongo.close
  		mongo2.close
  	} }
  }
	
	def main(args: Array[String]): Unit = {
		val c = Calendar.getInstance
		c.setTime(sdf.parse("20171015"))
	 	doBackUp(c)
	 	//150217
	}
}