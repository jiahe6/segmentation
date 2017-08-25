package org.gz.mongospark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.bson.Document
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.regex
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._
import com.mongodb.client.model.Aggregates._
import com.mongodb.MongoClientOptions
import org.gz.getershen

/**
 * @author cloud
 *	spark连接mongodb的代码
 *  一审再审数据导入
 */
object MoSpark { 
	lazy val mongo = new MongoClient("192.168.12.161", 27017)
	lazy val db = mongo.getDatabase("wenshu")
	lazy val dbColl = db.getCollection("origin2")
	
	import scala.collection.JavaConversions._
	val arr = Array("_id", "content", "encoding", "filepath", "case_type", "casecause", "filepath", "fulltxt", "judge_step")
	
	/**
	 * 将没用的字段去掉，并把字段名改至标准名称
	 */
	def updateDocument(dd: Document) = {
		val doc = dd
		arr.foreach(x => doc.remove(x))
		var d = new Document
		doc.keySet().foreach(key => {
			if (doc.getString(key) != ""){
				key match {
					case "一审经过"|"前审经过" =>
						var 一审经过 = new Document
						一审经过.append("全文", doc.getString(key))
						d.append("一审经过", 一审经过)
					case "原告诉称" => d.append("诉称", doc.getString(key))
					case "被告辩称" => d.append("辩称", doc.getString(key))
					case _ => d.append(key, doc.getString(key))
				}				
			}
		})
		d
	}
	
	def updateBasicLabel(doc: Document) = {
		val arr = Array("_id", "content", "encoding", "filepath")
		arr.foreach(x => doc.remove(x))
		doc
	}

	
  def main(args: Array[String]): Unit = {
  	//System.setProperty("hadoop.home.dir", "D:/hadoop-common")
    val spark = SparkSession.builder()
    	.master("spark://192.168.12.161:7077")
    	.config(new SparkConf().setJars(Array("hdfs://192.168.12.161:9000/mongolib/mongo-spark-connector_2.11-2.0.0.jar",
    			"hdfs://192.168.12.161:9000/mongolib/bson-3.4.2.jar",
    			"hdfs://192.168.12.161:9000/mongolib/mongo-java-driver-3.4.2.jar",
    			"hdfs://192.168.12.161:9000/mongolib/mongodb-driver-3.4.2.jar",
    			"hdfs://192.168.12.161:9000/mongolib/mongodb-driver-core-3.4.2.jar",
    			"hdfs://192.168.12.161:9000/mongolib/commons-io-2.5.jar",
    			"hdfs://192.168.12.161:9000/segwithorigin2.jar")))
    	.config("spark.cores.max", 60)		
    	.config("spark.executor.cores", 12)
    	.config("spark.executor.memory", "32g")
    	.config("spark.mongodb.input.uri", "mongodb://192.168.12.161:27017/wenshu.segdata3_jh")
 //   	.config("spark.mongodb.output.uri", "mongodb://192.168.12.161:27017/wenshu.origin2")
 //   	.config("spark.mongodb.input.partitioner", "MongoShardedPartitioner")
    	.getOrCreate()
    
   	val rdd = MongoSpark.builder().sparkSession(spark).build.toRDD()
   	rdd.foreach{ x => {
   		try{
   			dbColl.updateOne(eqq("_id", x.get("_id")), set("segdata", updateDocument(x)))
   		}catch{
   			case e: Throwable => e.printStackTrace()
   		}
   	}}
//    mongo.close()
//    	val df = MongoSpark.load(spark)
//    	println(df.schema)
//   	import spark.implicits._
//   	rdd.foreach { x => {
//   		val id = x.get("_id")
//   		dbColl.updateOne(eqq("_id", id), set("basiclabel", updateBL(x))) 		
//   	}}
  	
//   	val docs = rdd.map { doc => doc.get("分段结果", classOf[java.util.List[String]])}

  	
  }
}