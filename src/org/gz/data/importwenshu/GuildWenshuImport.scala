package org.gz.data.importwenshu

import org.gz.ImportOrigin
import java.io.File
import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import org.bson.Document
import scala.collection.mutable.ArrayBuffer
import javax.swing.text.rtf.RTFEditorKit
import java.io.FileInputStream
import javax.swing.text.DefaultStyledDocument
import scala.io.Source
import scala.collection.mutable.HashSet

object GuildWenshuImport {
	
	lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo.getDatabase("wenshu")
	private lazy val dbColl = db.getCollection("guidingwenshu")
	val set = Array[String]("关键词", "裁判要点", "相关法条", "基本案情", "裁判结果", "裁判理由")
	
	def segGuilding(iter: Iterator[String]) = {
		val map = new ArrayBuffer[(String, String)]
		iter.foreach{x =>
			val str = x.replace(12288.toChar, ' ').replace('【', ' ').replace('】', ' ').trim()			
			if (str.startsWith("指导案例")) map += (("_id", str)) else{				
				var flag = false
				set.foreach { y =>					
					if (str.startsWith(y)) {
						val z = str.substring(y.length(), str.length()).trim
						if (z.length() > 0)
							if ((z(0) == ':')||z(0) == '：')
								map += ((y, z.substring(1, z.length())))
							else map += ((y, z))
						else map += ((y, ""))
						flag = true
					}}
				if (!flag) map(map.length - 1) = (map(map.length - 1)._1, map(map.length - 1)._2 + "\n" + x)
			}
		}
		val doc = new Document()
		map.foreach(x => {
			doc.append(x._1, x._2)
		})		
		doc
	}
	
	def importwenshu(f: File) = {
		val doc = new Document()
		val map = new ArrayBuffer[(String, String)]
		var content = ""
		Source.fromFile(f, "GBK").getLines().foreach{x =>
			content = content + "\n" + x
			val str = x.replace("\uFEFF", "").replace(12288.toChar, ' ').replace('【', ' ').replace('】', ' ').trim()			
			if (str.startsWith("指导案例")) map += (("_id", str)) else if (str != ""){				
				var flag = false
				set.foreach { y =>					
					if (str.startsWith(y)) {
						val z = str.substring(y.length(), str.length()).trim
						if (z.length() > 0)
							if ((z(0) == ':')||z(0) == '：')
								map += ((y, z.substring(1, z.length())))
							else map += ((y, z))
						else map += ((y, ""))
						flag = true
					}}
				if (!flag) map(map.length - 1) = (map(map.length - 1)._1, map(map.length - 1)._2 + "\n" + str)
			}
		}		
		map.foreach(x => {
			println(x)
			doc.append(x._1, x._2)
		})		
		doc.append("content", content)
		dbColl.insertOne(doc)
		println("\n")
	}
	
	def main(args: Array[String]): Unit = {
		val files = ImportOrigin.getAllFiles(new File("C:/Users/cloud/Desktop/类案搜索/数据分析杨天泰和数据组资料/指导性案例/"))		
		files.foreach {importwenshu}
		//val f = new File("C:/Users/cloud/Desktop/类案搜索/数据分析杨天泰和数据组资料/指导案例1号：上海中原物业顾问有限公司诉陶德华居间合同纠纷案.txt")
		//importwenshu(f)
		mongo.close()		
	}
}