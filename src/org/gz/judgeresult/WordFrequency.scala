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

/**
 * 盗窃罪，先抽样，再获取初分结果
 */
object WordFrequency {
	lazy val client = new MongoClient("192.168.12.147", 65426)
	lazy val db = client.getDatabase("wenshu")
	lazy val dbColl = db.getCollection("theft")
	val theftSample = "D:/library/sampleout/theftSample/"

	//词典writer，通过initclassify获取词典，然后对每个词典生成一个printwriter，每3000条换一个文件夹
	val dictWriter = HashMap[String, PrintWriter]()
	//没有路径那么就创建整个路径
	checkFileParent(new File(theftSample + "others"))
	val printOther = new PrintWriter(new FileWriter(new File(theftSample + "others"), true))
	
	//加载词典
	def __init__(i: Int) = {
		dictWriter.foreach(_._2.close)
		InitClassify.dict.foreach(x =>{ 
			val file = new File(theftSample + i.toString + "/" + x)
			checkFileParent(file)
			dictWriter += ((x, new PrintWriter(new FileWriter(file, true))))			
		})
	}
	__init__(0) 
	
	//检查父目录是否有这个路径，没有就创建
	def checkFileParent(file: File) = if (!file.getParentFile.exists) file.getParentFile.mkdirs
	
	//f: Document -> unit 
	//把没经过初分的数据输出出来，正式使用时不用此函数用sample2
	@deprecated("使用sample2对结果进行初分")
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
	
	//f : Document -> unit
	//获取一个Dcoument的正文，然后经过正则表达式匹配，由对应的pritwriter写到指定文件
	def doSample2(doc: Document) = {    
		val content = doc.getString("content")    
	  val seg = doc.get("segdata", classOf[Document])
	  if (seg != null) {
	  	val judgeRes = seg.getString("裁判结果")
	  	if (judgeRes != null){
	  		val c = 11.toChar
	  		judgeRes.split(s"[${c}\n]").foreach(y => 
	  			seperateSentence(y.replace("\r", "")).foreach(x => 
	  			if ((x != "")&&(!filterOtherSentence(x))) {
	  				val writer = dictWriter.getOrElse(InitClassify.classify(x), printOther)
	  				writer.println(x)
	  			}))
	  	}
	  }
	}
	
	//把套话过滤了，暂时没用
	def filterOtherSentence(str: String) = {
		var flag = false
		val otherSentence = Array(
				"本裁定送达后立即发生法律效力",
				"如不服本判决",
				"可在接到判决书的第二日起十日内",
				"书面上诉的",
				"本裁定为终审裁定",
				"本判决为终审判决",
				"本裁定送达后即发生法律效力",
				"本裁定自送达之日起发生法律效力"
				)
		val otherSentenceRegex = Array(
				"通过本院或[^:：,。.，]*提(起|出)上诉",
				"应当?提?交上诉状正本.?份",
				"副本.?份"
				)		
		otherSentence.foreach { x => flag = flag | str.contains(x)}
		otherSentenceRegex.foreach( x => {
			val m = x.r.findAllIn(str)
			if (m.hasNext) flag = true
		})  
		flag
	}
	
	//给定一个mongodb的返回结果，再给定一个文档处理函数，再给定一个随机数跨度，来进行抽样
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
	
	//判断一个字符是否出现在句子里，用来给后面递归判断括号用，应该有更好的方法，到时候替换就行
	def isin(x: Char, str: String) = {
		var flag = false
		str.foreach(y => if (x == y) flag = true)
		flag
	}
	
	//对一个段落先把它中间不规范的数字转化为规范数字，然后删除括号内容，然后处理特殊情况，返回的是一段话的断句结果
	def seperateSentence(ss: String) = {		
		//将4，800、300,200.00元这种分出来，转化为标准写法
		val regex = "([0-9]+[，、,.])+[0-9]+".r
		var str = ss.trim
		val m = regex findAllIn str
		m.foreach(z => {
			val za = z.replaceAll("[，、,]", "")
			str = str.replace(z, za)
		}) 
		//将括号内数据分隔出来，加入到一个ArrayBuffer中，然后删除原句中的括号内容。
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
	
	//正式使用时没用到
	@deprecated("临时数据的处理方法")
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