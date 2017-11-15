package org.gz

import com.mongodb.MongoClient
import scala.collection.JavaConversions.asScalaIterator
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils

/**
 * 用袁媛的数据库查看原始库中被删了哪些数据
 */
object FindMissedData {
	
	lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	lazy val mongo = new MongoClient(mongoURI)
	val db = mongo.getDatabase("wenshu")
	val dbColl = db.getCollection("origin2")
	val dbCollyy = db.getCollection("daoqie_yy")
	
  def main(args: Array[String]): Unit = {
		var missed = 0
    dbCollyy.find.iterator.foreach(x => {
    	val res = dbColl.find(eqq("_id", x.getString("_id"))).iterator
    	if (!res.hasNext()){
    		println(x.getString("_id"))
    		missed = missed + 1
    	}
    })
    println(missed)
  }
}