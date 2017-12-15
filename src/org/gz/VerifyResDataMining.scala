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
import scala.io.Source

object VerifyResDataMining {
  private lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo getDatabase "datamining"
	private lazy val dbColl = db getCollection "sampledata"
	val sampleCount = 100	
	
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
		println(count)
		docs
  }
  
  def containsKeyWord(sentence: String, key: String) = {
  	val words = key.split(",|，")
  	var flag = true
  	words.foreach{x => 
  		flag = flag & sentence.contains(x)
  	} 
  	flag
  }
  
  def getSentence(doc: Document): String = {
  	val segdata = doc.get("segdata", classOf[Document])
  	var sentence = ""
  	if (segdata != null)  	
  		segdata.keySet().foreach { x =>
  			if ((x != "当事人")&&(x != "附")&&(x != "一审经过")&&(x != "全文")){
  				val str = segdata.getString(x)
  				if (str != null) sentence = sentence + "\n" + str
  			} else if (x == "一审经过") {
  				val 一审 = segdata.get(x, classOf[Document])
  				if (一审 != null){
  					val str = 一审.getString("全文")
  					if (str != null) sentence = sentence + "\n" + str
  				}
  			}
  		}
  	sentence
  }
  
  def analyzeSampleData(casecause: String, dict: HashMap[String, (String, String)], outFile: File) = {
  	val docs = sampledata(casecause)  	
  	val segWriter = new PrintWriter(outFile)
  	val c = 11.toChar
  	docs.foreach { x => 
  		val mininglabel = x.get("mininglabel", classOf[Document])
  		val sentence = getSentence(x)
  		if (sentence != null){  			
  			segWriter.write("----------------------------------------------------\n" + x.getString("_id") + "\n")
  			val sentences = sentence.split(s"[${c}\n。]")
  			val list = mininglabel.get("案件特征", classOf[java.util.ArrayList[String]])
  			val set = if (list != null) HashSet(list: _*) else HashSet[String]()
  			sentences.foreach { y =>
  				dict.foreach{z =>
  					if ((containsKeyWord(y, z._1))&&(!set.contains(z._2._2))){  						
  						segWriter.write("\n" + y + "\n")
  						segWriter.write(z._1 + "->" + z._2._1 + "\n")
  						segWriter.write("已有标签: " + set.mkString(", ") + "\n")  						
  					}}}
  		}}
  	segWriter.close()
  }
  
  def analyzeSampleDataZhengyi(casecause: String, dict: HashMap[String, (String, String)], outFile: File) = {
  	val docs = sampledata(casecause)  	
  	val segWriter = new PrintWriter(outFile)
  	val c = 11.toChar
  	docs.foreach { x => 
  		val mininglabel = x.get("mininglabel", classOf[Document])
  		val sentence = mininglabel.getString("争议焦点句子")
  		if (sentence != null){  			
  			segWriter.write("----------------------------------------------------\n" + x.getString("_id") + "\n")
  			val sentences = sentence.split(s"[${c}\n。]")
  			val list = mininglabel.get("争议焦点", classOf[java.util.ArrayList[String]])
  			val set = if (list != null) HashSet(list: _*) else HashSet[String]()
  			sentences.foreach { y =>
  				dict.foreach{z =>
  					if ((containsKeyWord(y, z._1))&&(!set.contains(z._2._2))){  						
  						segWriter.write("\n" + y + "\n")
  						segWriter.write(z._1 + "->" + z._2._1 + "\n")
  						segWriter.write("已有标签: " + set.mkString(", ") + "\n")  						
  					}}}
  		}}
  	segWriter.close()
  }
  
  def main(args: Array[String]): Unit = {
//    val dict = HashMap[String, String](
//			"借款" -> "借款关系争议",
//			"借贷" -> "借款关系争议",
//			"贷款" -> "借款关系争议",
//			"交付" -> "借款关系争议",
//			"金额" -> "借款数额争议",
//			"数额" -> "借款数额争议",
//			"用于" -> "借款用途争议",
//			"效力" -> "合同效力争议",
//			"诉讼" -> "虚假诉讼争议",
//			"还款" -> "还款责任争议",
//			"连带" -> "连带责任争议",
//			"转让" -> "债权债务转让争议"			
//			)
  	val dict = HashMap[String, (String, String)]()
  	val filePath = "C:/Users/cloud/Desktop/类案搜索/数据分析杨天泰和数据组资料/数据清洗/"
  	val filePath2 = "C:/Users/cloud/Desktop/类案搜索/数据分析杨天泰和数据组资料/数据清洗结果/"
  	val path = new File(filePath)
  	path.list().foreach { fileName =>  		
  		dict.clear()
	  	val f = new File(filePath + fileName)
			Source.fromFile(f, "utf-8").getLines().foreach { x =>
				val arr = x.split("->")
				var str = arr(1)
				for (i <- 2 until arr.length) str = str + "->" + arr(i)
				dict += ((arr(0), (str, arr(arr.length-1))))				
			}
	  	dict.foreach(println)
	  	val casecause = if (fileName.endsWith(".txt")) fileName.substring(0, fileName.length()-4) else fileName
			analyzeSampleData(casecause, dict, new File(filePath2 + fileName + "抽取"))
	  	//analyzeSampleDataZhengyi(casecause, dict, new File(filePath2 + fileName + "抽取争议"))
  	}
  }
	
}