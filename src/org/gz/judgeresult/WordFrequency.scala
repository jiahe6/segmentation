package org.gz.judgeresult

import com.mongodb.MongoClient
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.asScalaBuffer
import org.bson.Document
import scala.util.Random
import java.io.PrintWriter
import java.io.File
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import scala.collection.mutable.HashMap


object WordFrequency {
	lazy val client = new MongoClient("192.168.12.147", 65426)
	lazy val db = client.getDatabase("wenshu")
	lazy val dbColl = db.getCollection("theft")
	val theftSample = "D:/library/sampleout/theftSample/"

	val dictWriter = HashMap[String, PrintWriter]()
	checkFileParent(new File(theftSample + "others"))
	val printOther = new PrintWriter(new FileWriter(new File(theftSample + "others"), true))
	
	def __init__(i: Int) = {
		dictWriter.foreach(_._2.close)
		InitClassify.dict.foreach(x =>{ 
			val file = new File(theftSample + i.toString + "/" + x)
			checkFileParent(file)
			dictWriter += ((x, new PrintWriter(new FileWriter(file, true))))			
		})
	}
	__init__(0) 
	
	def checkFileParent(file: File) = if (!file.getParentFile.exists) file.getParentFile.mkdirs
	
	def doSample(doc: Document) = {
		val resultWriter = new PrintWriter(new File("D:/library/sampleout/theftResult/" + doc.getString("_id")))
    val contentWriter = new PrintWriter(new File("D:/library/sampleout/theftContent/" + doc.getString("_id")))
    val content = doc.getString("content")
    
	  val seg = doc.get("segdata", classOf[Document])
	  if (seg != null) resultWriter.write({
	  	val w = seg.getString("裁判结果")
	  	if (w != null) w else ""
	  })
    contentWriter.write(content)
    resultWriter.close()
    contentWriter.close()
	}
	
	def doSample2(doc: Document) = {    
    val content = doc.getString("content")    
	  val seg = doc.get("segdata", classOf[Document])
	  if (seg != null) {
	  	val judgeRes = seg.getString("裁判结果")
	  	if (judgeRes != null){
	  		val c = 11.toChar
	  		judgeRes.split(s"[${c}\n]").foreach(y => 
	  			seperateSentence(y.replace("\r", "")).foreach(x => 
	  			if (x != "") {
	  				val writer = dictWriter.getOrElse(InitClassify.classify(x), printOther)
	  				writer.println(x)
	  			}))
	  	}
	  }
	}
	
	def sampleData(docs: Iterator[Document], f: Document => Unit, n: Int = 100) = {
		var r = new Random
		def nextint = r.nextInt(n) + 1
		var doc : Document = null
		var count = 0
		var total = 0
		var i = nextint
		while (docs.hasNext){
    	doc = docs.next()
    	if (count == i){ 
    		i = i + nextint
	    	f(doc)
    		total = total + 1
    		if (total % 3000 == 0) __init__(total / 3000)
    	}
    	count = count + 1
    }		
	}
	
	def isin(x: Char, str: String) = {
		var flag = false
		str.foreach(y => if (x == y) flag = true)
		flag
	}
	
	def seperateSentence(ss: String) = {		
		//将4，800、300,200.00元这种分出来
		val regex = "([0-9]+[，、,.])+[0-9]+".r
		var str = ss.trim
		val m = regex findAllIn str
		m.foreach(z => {
			val za = z.replaceAll("[，、,]", "")
			str = str.replace(z, za)
		}) 
		//将括号内数据分隔出来
		var i = 0		
		var stack = new java.util.Stack[Int]	//存放左括号
		var res = new ArrayBuffer[String]()		//存放数据结果
		val left = "\\[\\(\\{（［﹝﹙〔【<﹤〈"
		val right = "\\]\\)\\}）］﹞﹚〕】>﹥〉"
		while (i < str.length){
			if (isin(str(i), left)) stack.push(i) 
				else if(isin(str(i), right)) {
					try{
						val l = stack.pop
						if (i - l > 2) res += str.substring(l, i+1)						
					}catch{
						case e: Throwable => 
					}
					
				}
			i = i + 1
		}
//		println("str = " + str)
//		res.foreach { x => println("res = " + x) }
		res.reverse.foreach(x => str = str.replace(x, ""))
		//追缴和没收和扣押不能以逗号断句
		var arr = str.split("。")
		var sentence = ArrayBuffer[String]()
		var 追缴没收扣押 = "追缴|没收|扣押|收缴|驳回.*维持原判".r
		arr.foreach(x => {
			val res2 = 追缴没收扣押.findAllIn(x)
			if (res2.hasNext) sentence += x else sentence ++= x.split("，|。|；|;")	
		})
		//把括号内的东西都去掉了，如果需要加回来使用 "++ res"
		//以逗号和句号断句
		sentence	
	}
	
	def getFiles(path : File) = {
		path.listFiles().foreach(x => {
			val printer = new PrintWriter(new File("D:/library/sampleout/theftAnnotated/" + x.getName))
			Source.fromFile(x).getLines.foreach(y => {
				//如果一行全部内容都是由括号括起来的，有可能有空字符串出现
				seperateSentence(y.replace("\r", "")).foreach(z => {if (z != "") printer.write(z.trim + "\t->\t\n")})
			})
			printer.close
		})
	}
	
  def main(args: Array[String]): Unit = {
//		getFiles(new File("D:/library/sampleout/theftResult/"))
//		val m = seperateSentence("""一、被告人杜某某犯盗窃罪，判处有期徒刑八个月，并处罚金一千元""")
//		for (i <- 0 until m.length){
//			if (m(i) != "") 
//				println(i + "->" + m(i))
//		}
		sampleData(dbColl.find.iterator, doSample2)
		printOther.close
		dictWriter.foreach(_._2.close)
  }
}