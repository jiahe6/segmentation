package org.gz

import com.mongodb.MongoClient
import scala.collection.JavaConversions.asScalaIterator
import com.mongodb.client.model.Filters.{eq => eqq}

object FindMissedData {
	
	val cli = new MongoClient("192.168.12.161", 27017)
	val db = cli.getDatabase("wenshu")
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