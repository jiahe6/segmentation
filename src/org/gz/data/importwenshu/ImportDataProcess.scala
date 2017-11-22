package org.gz.data.importwenshu

import org.bson.Document
import main.DocHandlerMongoToMongo
import tp.file.label.FindLabelByMongo
import java.text.SimpleDateFormat
import java.util.Date
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}

object ImportDataProcess {
	
	def processBasicData(d: Document): Document = {
		FindLabelByMongo findBasicLabel d		
	}
	
	def processSegData(d: Document) = {
		val doc = d
 	  val seg = new DocHandlerMongoToMongo
    val segdoc = seg genSegData doc
    var basiclabel = doc.get("basiclabel", classOf[Document])
    if (basiclabel != null) {    	
    	val lsls = segdoc.get("basiclabel.律师律所", classOf[Document])
    	if (lsls != null) (basiclabel.append("律师律所", lsls))
    	val spry = segdoc.get("basiclabel.审判人员", classOf[Document])
    	if (spry != null) (basiclabel.append("审判人员", spry))
    	val dsr = segdoc.get("basiclabel.当事人", classOf[Document])
    	if (dsr != null) (basiclabel.append("当事人", dsr))
    }
    val segdata = segdoc.get("segdata", classOf[Document])
    doc.append("basiclabel", basiclabel)
    if (segdata != null) (doc.append("segdata", segdata))
    doc
	}
	
	def processData(d: Document) = {
//		val timesdf = new SimpleDateFormat("yyyyMMdd  HH:mm:ss:SSS")
//		println(timesdf.format(new Date(System.currentTimeMillis())) + ": start findLabel")
		val doc = processBasicData(d)
//		println(timesdf.format(new Date(System.currentTimeMillis())) + ": end findLabel")
		processSegData(doc)
	}
	
	def main(args: Array[String]): Unit = {
		val muu = new MongoUserUtils
		val mongoURI = new MongoClientURI(muu.clusterMongoURI)
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase("wenshu")
		val dbColl = db.getCollection("origin2")
		val rs = dbColl.find(eqq("_id", "8365f7ef-d676-4045-828e-739e1defe239")).iterator()		
	  val d = processSegData(rs.next())
	  println(d.get("segdata", classOf[Document]))
	  
	}
}