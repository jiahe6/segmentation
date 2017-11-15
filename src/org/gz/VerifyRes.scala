package org.gz

import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import org.bson.Document
import java.util.ArrayList
import scala.util.Random
import java.io.File
import java.io.PrintWriter
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.gz.mongospark.SegWithOrigin2
import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils

/**
 * 抽样数据，进行分段结果评测
 */
object VerifyRes {
	
	private lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo getDatabase "wenshu"
	private lazy val dbColl = db getCollection "sample_gz"
	private lazy val dbColl2 = db getCollection "origin2"
	
	def sample = {
		val docs = dbColl2 find eqq("basiclabel.procedure", "二审") noCursorTimeout true iterator
		var r = new Random
    def nextInt = r.nextInt(480) + 1
    var i = nextInt
    var count = 0
    var doc = new Document() 
    var ins = 0
    while (docs.hasNext()){
    	doc = docs.next()
    	if (count == i){
    		ins = ins + 1
    		i = i + nextInt
	    	dbColl.insertOne(doc)	    	
    	}
    	count = count + 1
    	if (count % 10000 == 0) println(count + ": hit\t" + ins)
    }
	}
	
	def verify(n: Int) = {
		val docs = dbColl find eqq("basiclabel.procedure", "二审") noCursorTimeout true iterator
		var r = new Random
    def nextInt = r.nextInt(n) + 1
    var i = nextInt
    var count = 0
    var doc = new Document() 
    while (docs.hasNext()){
    	doc = docs.next()
    	if (count == i){
    		i = i + nextInt
	    	val segWriter = new PrintWriter(new File("D:/library/sampleout/seg3/" + doc.getString("_id")))
    		val contentWriter = new PrintWriter(new File("D:/library/sampleout/content3/" + doc.getString("_id")))
    		val content = doc.getString("content")
	    	val seg = doc.get("segdata", classOf[Document])
    		seg.get("全文", classOf[java.util.List[Document]]).foreach(x => segWriter.write(x.toString + "\n\n"))
    		contentWriter.write(content)
    		segWriter.close()
    		contentWriter.close()
    	}
    	count = count + 1
    }
	}
	
	val documents = ArrayBuffer[Document]()
	def getDocsFromFileList(f: File): Unit = {
		if (f.isDirectory) f.listFiles().foreach(getDocsFromFileList) 
			else if (f.isFile) {
				val id = f.getName
				println(id)
				val docs = dbColl find eqq("_id", id) iterator
				
				documents += docs.next()
			}
	}
	
	def segError() = {
		val c = 11.toChar
		documents.foreach( x => { 
			val str = x.getString("content").split(s"[${c}\n]")
			val d = SegWithOrigin2.segment(str)
			val arr = d.get("全文", classOf[java.util.List[Document]])
    	val segWriter = new PrintWriter(new File("D:/library/sampleout/errorseg2/" + x.getString("_id")))
			arr.foreach(y => segWriter.write(y.toString + "\n\n"))
			segWriter.close
		})
	}
	
  def main(args: Array[String]): Unit = {
    //val docs = dbColl find eqq("basiclabel.procedure", "二审") noCursorTimeout true iterator
//  	getDocsFromFileList(new File("D:/library/sampleout/error2"))
//  	segError

    verify(40)
    mongo.close
  }
}