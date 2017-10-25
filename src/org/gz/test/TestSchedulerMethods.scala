package org.gz.test

import org.gz.data.importwenshu.ScheduleImport
import java.util.Calendar
import java.text.SimpleDateFormat
import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient

object TestSchedulerMethods {
	
	def testCount = {
		//907582
		val mongoURI2 = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
		val mongo2 = new MongoClient(mongoURI2)
		val db2 = mongo2.getDatabase("wenshu")
		val dbColl2 = db2.getCollection("origintest")
		var res = 0
		val iter = dbColl2.find().iterator()
		while (iter.hasNext()){
			iter.next()
			res = res + 1
		}
		println(res)
	}
	
	def testInsert = {
		val c = Calendar.getInstance
		val sdf = new SimpleDateFormat("yyyyMMdd")
		c.setTime(sdf.parse("20171023"))
  	ScheduleImport.doInsertByTime(c)
	}
	
	def main(args: Array[String]): Unit = {
	  testCount
	}
}