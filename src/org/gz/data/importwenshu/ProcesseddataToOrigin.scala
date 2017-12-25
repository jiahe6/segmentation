package org.gz.data.importwenshu

import org.gz.util.MongoUserUtils
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import org.bson.Document
import scala.collection.JavaConversions._
import org.apache.logging.log4j.LogManager

object ProcesseddataToOrigin {
	
	val log = LogManager.getLogger(this.getClass.getName())
	private val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private	val mongo = new MongoClient(mongoURI)

	def moveData(fromColl: MongoCollection[Document], dest: MongoCollection[Document]) = {
		val iter = fromColl.find()//.noCursorTimeout(true)
		var count = 0
		iter.foreach(x => {
			count = count + 1
			try{
				dest.insertOne(x)
			}catch {
				case e: Throwable => 
			}
			if ((count % 10000) == 0) {
				println(count)
			}
		})
		
	}
	
 	def main(args: Array[String]): Unit = { 		
 		val db = mongo.getDatabase("wenshu")
		val dbColl = db.getCollection("origin2")
		val dbfrom = mongo.getDatabase("updatesdata")
		val dbCollfrom = dbfrom.getCollection("processeddata")
		moveData(dbCollfrom, dbColl)
	}
}