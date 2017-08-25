package org.gz

import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import scala.collection.JavaConversions.asScalaIterator
import org.bson.Document

object CaseNumber {
	lazy val client = new MongoClient("192.168.12.161", 27017)
	lazy val db = client.getDatabase("wenshu")
	lazy val dbColl = db.getCollection("origin2")

	def main(args: Array[String]): Unit = {
		val docs = dbColl.find(eqq("basiclabel.casecause", "盗窃罪")).iterator()	
  	val regex = "[\\[\\(\\{（［﹝﹙〔【<﹤〈]?([1１l][9９]|[2２][0０Oo])[0-9０-９oOl][0-9０-９oOl][\\]\\)\\}）］﹞﹚〕】>﹥〉]?[\\u4e00-\\u7b2b\\u7b2d-\\u9fa50-9０-９()（）\\d\\-×xXＸ＊olO－—0-9０-９\\[\\(\\{（［﹝﹙〔【<﹤〈\\]\\)\\}）］﹞﹚〕】>﹥〉]{3,10}第?[oOl0-9０-９某＊※XxＸｘ╳×Ⅹ\\*\\d]+([—－\\-][oOl0-9０-９某＊※XxＸｘ╳×Ⅹ\\*\\d]+)*号".r
  	val reg22 = "[\\[\\(\\{（［﹝﹙〔【<﹤〈]([1１l][9９]|[2２][0０Oo])[0-9０１２３４５６７８９oOl][0-9０１２３４５６７８９oOl][\\]\\)\\}）］﹞﹚〕】>﹥〉][\\u4e00-\\u7b2b\\u7b2d-\\u9fa50-9()（）\\d\\-×xXＸ＊olO－—０１２３４５６７８９\\[\\(\\{（［﹝﹙〔【<﹤〈\\]\\)\\}）］﹞﹚〕】>﹥〉]{3,10}第?[oOl０１２３４５６７８９某＊Xx╳×\\*\\d]+([—－\\-][oOl０１２３４５６７８９某＊Xx╳×\\*\\d]+)*号".r
  	docs.foreach(x => {
  		val basic = x.get("basiclabel", classOf[Document])
  		val caseNumber = basic.getString("caseid").trim()
  		val content = x.getString("content")
  		val iter = reg22.findAllIn(content)
  		iter.foreach { y => println(x.getString("_id") + "\t" + y) }
  	})
	}
}