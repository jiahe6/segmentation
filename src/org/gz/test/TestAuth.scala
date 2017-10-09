package org.gz.test

import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.File
import scala.collection.JavaConverters._
import java.io.PrintWriter
import org.gz.struct.DFFG
import org.gz.DFFGc
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf

object TestAuth {

	val mongoURI = new MongoClientURI("mongodb://gaoze:qazwsxedc!@192.168.12.161:27017/?authSource=admin")
	//val mongoURI = new MongoClientURI("mongodb://192.168.12.148:27017/");
	lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo.getDatabase("wenshu")
	private lazy val dbColl = db.getCollection("origin2")
	
	def testJavaApi = {
		val res = dbColl.find().iterator()
  	import scala.collection.JavaConversions._
  	println(res.next())
	}
	
	def testSpark = {
		  	val spark = SparkSession.builder()
//  		.config("spark.mongodb.input.uri", "mongodb://gaoze:Dx72000000!@192.168.12.148:27017/test.test?authSource=admin")
  		.master("local").getOrCreate()
  	val readConfig = ReadConfig(Map(
  			"uri" -> "mongodb://rw:1@192.168.12.148:27017/?authSource=test",
  			"database" -> "test",
  			"collection" -> "test"
  			))
  	val rdd = MongoSpark.builder().sparkSession(spark).readConfig(readConfig).build().toRDD()
  	val r2 = MongoSpark.load(spark.sparkContext, readConfig)
  	
  	rdd.collect().foreach(x => {
  		println(x.getString("_id") + ":	" + x.getString("line1"))
  		
  	})

	}
	
  def main(args: Array[String]): Unit = {
  	testJavaApi
  }
}