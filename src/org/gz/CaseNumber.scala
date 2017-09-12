package org.gz

import com.mongodb.MongoClient
import com.mongodb.client.model.Updates.set
import com.mongodb.client.model.Updates.unset
import com.mongodb.client.model.Filters.{eq => eqq}
import scala.collection.JavaConversions.asScalaIterator
import org.bson.Document
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object CaseNumber {
	lazy val client = new MongoClient("192.168.12.147", 65426)
	lazy val db = client.getDatabase("wenshu")
	lazy val dbColl = db.getCollection("theft")

	def setCaseNumber = {
		val docs = dbColl.find(eqq("basiclabel.casecause", "盗窃罪")).iterator()	
  	val regex = "[\\[\\(\\{（［﹝﹙〔【<﹤〈]?([1１l][9９]|[2２][0０Oo])[0-9０-９oOl][0-9０-９oOl][\\]\\)\\}）］﹞﹚〕】>﹥〉]?[\\u4e00-\\u7b2b\\u7b2d-\\u9fa50-9０-９()（）\\d\\-×xXＸ＊olO－—0-9０-９\\[\\(\\{（［﹝﹙〔【<﹤〈\\]\\)\\}）］﹞﹚〕】>﹥〉]{3,10}第?[oOl0-9０-９某＊※XxＸｘ╳×Ⅹ\\*\\d]+([—－\\-][oOl0-9０-９某＊※XxＸｘ╳×Ⅹ\\*\\d]+)*号".r
  	val reg22 = "[\\[\\(\\{（［﹝﹙〔【<﹤〈]([1１l][9９]|[2２][0０Oo])[0-9０１２３４５６７８９oOl][0-9０１２３４５６７８９oOl][\\]\\)\\}）］﹞﹚〕】>﹥〉][(\\u4e00-\\u7b2b\\u7b2d-\\u9fa5)][\\u4e00-\\u7b2b\\u7b2d-\\u9fa50-9()（）\\d\\-×xXＸ＊olO－—０１２３４５６７８９\\[\\(\\{（［﹝﹙〔【<﹤〈\\]\\)\\}）］﹞﹚〕】>﹥〉]{2,9}第?[oOl０１２３４５６７８９某＊Xx╳×\\*\\d]+([—－\\-][oOl０１２３４５６７８９某＊Xx╳×\\*\\d]+)*号".r
  	docs.foreach(x => {
  		val basic = x.get("basiclabel", classOf[Document])
  		val caseNumber = basic.getString("caseid").trim()
  		val content = x.getString("content")
  		val iter = reg22.findAllIn(content)
  		val arr = ArrayBuffer[String]()
  		iter.foreach { y =>
  			if (y != caseNumber){
  				println(x.getString("_id") + "\t" + y)
  				arr += y
  			}
  		}  		
  		if (arr.length > 0)
  			dbColl.updateOne(eqq("_id", x.get("_id")), set("mininglabel.相关案号", arr.toList.asJava))
   	})
	}
	
	def unsetCaseNumber = {
		val docs = dbColl.find(eqq("basiclabel.casecause", "盗窃罪")).iterator()
		docs.foreach(x => {
			val l = x.get("mininglabel", classOf[Document]).get("相关案号", classOf[java.util.List[String]])
			if (l.size() == 0)
				dbColl.updateOne(eqq("_id", x.get("_id")), unset("mininglabel.相关案号"))
			else println(x.getString("_id") + "\t" + l.asScala.mkString(","))
		})		
	}
	
	def unset相关法条 = {
		val docs = dbColl.find(eqq("basiclabel.casecause", "盗窃罪")).iterator()
		docs.foreach(x => {
				dbColl.updateOne(eqq("_id", x.get("_id")), unset("mininglabel.相关法条"))			
		})		
	}
	
	def main(args: Array[String]): Unit = {
		unset相关法条
		client.close()
	}
}