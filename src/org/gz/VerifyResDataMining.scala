package org.gz

import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Filters.{ne => nne}
import com.mongodb.client.model.Filters.and
import scala.util.Random
import org.bson.Document
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import java.io.File
import scala.collection.JavaConversions._

object VerifyResDataMining {
  private lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo getDatabase "datamining"
	private lazy val dbColl = db getCollection "sampledata"
	val sampleCount = 200	
	
	def sampledata(casecause: String) = {
		val count1 = dbColl.count(and(eqq("basiclabel.casecause", casecause), nne("mininglabel", null)))
		val step = Math.floor((count1 / sampleCount * 2 - 1)).toInt 
		println(count1)
		println(step)
		val iter = dbColl.find(and(eqq("basiclabel.casecause", casecause), nne("mininglabel", null))).iterator()
		var r = new Random
    def nextInt = r.nextInt(step) + 1
    var i = nextInt
    var count = 0
    var doc = new Document() 
		val docs = new ArrayBuffer[Document]()
    var ins = 0
    while (iter.hasNext()){
    	doc = iter.next()
    	if (count == i){
    		ins = ins + 1
    		i = i + nextInt
	    	docs += doc	    	
    	}
    	count = count + 1    	
    }
		docs
  }
    
  def analyzeSampleData(casecause: String, dict: HashMap[String, String]) = {
  	val docs = sampledata(casecause)  	
  	val segWriter = new PrintWriter(new File("C:/Users/cloud/Desktop/民间借贷纠纷"))
  	docs.foreach { x => 
  		val mininglabel = x.get("mininglabel", classOf[Document])
  		val sentence = mininglabel.getString("争议焦点句子")
  		if (sentence != null){
  			val sentences = sentence.split("\n")
  			val list = mininglabel.get("争议焦点", classOf[java.util.ArrayList[String]])
  			val set = if (list != null) HashSet(list: _*) else HashSet[String]()
  			sentences.foreach { y =>
  				dict.foreach{z =>
  					if ((y.contains(z._1))&&(!set.contains(z._2))){  						
  						segWriter.write("\n" + y + "\n")
  						segWriter.write(z._1 + "->" + z._2 + "\n")
  						segWriter.write("已有标签" + set.mkString(",") + "\n")
  						segWriter.write(x.getString("_id") + "\n")
  					}}}
  		}}
  	segWriter.close()
  }
  
  def main(args: Array[String]): Unit = {
    val dict = HashMap[String, String](
			"借款" -> "借款关系争议",
			"借贷" -> "借款关系争议",
			"贷款" -> "借款关系争议",
			"交付" -> "借款关系争议",
			"金额" -> "借款数额争议",
			"数额" -> "借款数额争议",
			"用于" -> "借款用途争议",
			"效力" -> "合同效力争议",
			"诉讼" -> "虚假诉讼争议",
			"还款" -> "还款责任争议",
			"连带" -> "连带责任争议",
			"转让" -> "债权债务转让争议"			
			)
		analyzeSampleData("民间借贷纠纷", dict)
  }
	
}