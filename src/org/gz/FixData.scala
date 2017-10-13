package org.gz

import java.util.Calendar
import java.text.SimpleDateFormat
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.sys.process._
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import org.gz.util.Conf
import java.io.File
import org.bson.Document
import java.util.ArrayList
import com.mongodb.client.model.InsertManyOptions
import org.apache.logging.log4j.LogManager

/**
 * @author cloud
 * 修复数据用，包含机器崩溃导致的解压错误以及数据被误删的重新插入，返回网站繁忙导致的数据包错误修复放在ScheduleImport中
 */
object FixData extends Conf{
	
	val log = LogManager.getLogger(this.getClass.getName())
	/**
	 * 根据日志找到没跑完的数据，并将其移动到一个新的文件夹内：
	 * 日志中存在具体路径，通过hashSet找到没出现过的日期，将其放入新的文件夹
	 * 由于每次都是专用方法，所以直接指定路径和表
	 */
	def fixDataByLog = {
		val logPath = "/home/cloud/origin/logs/warn.log"
    val wenshuPath = "/home/cloud/wenshupart"
    val newPath = "/home/cloud/wenshuremain/"
    val c = Calendar.getInstance
    val starttime = "20160201"
    val endtime = "20161201"
    val sdf = new SimpleDateFormat("yyyyMMdd")
    c.setTime(sdf.parse(starttime))
    val end = Calendar.getInstance
    end.setTime(sdf.parse(endtime))
    val processed = HashSet[String]()   
    val lines = Source.fromFile(logPath).getLines
    lines.foreach(x => {
    	val ll = x.split("/")
    	if (ll.length > 4)
    		processed += ll(ll.length-2)
    })
    
    def doMove(d: String) = {
    	val res = s"cp -r ${wenshuPath}/${d} ${newPath}".!
    	println(s"move $d res = $res")
    }
    
    while (c.before(end)){
    	if (!processed.contains(sdf.format(c.getTime)))
    		doMove(sdf.format(c.getTime))
    	c.add(Calendar.DAY_OF_MONTH, 1)
    }
	}
		
	/**
	 * 这玩意贼慢，慢到我也不知道得多久，尽量少用
	 */
	def fixDeletedData = {
		val mongoURI = new MongoClientURI("mongodb://gaoze:qazwsxedc!@192.168.12.161:27017/?authSource=admin")
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase("wenshu")
		val dborigin2 = db.getCollection("origin")
		val dbnewdata = mongo.getDatabase("updatesdata").getCollection("newdata")
		val rootf = new File(config.getString("importwenshu.linuxwenshupath"))
		rootf.listFiles.foreach(f => {
			val files = ImportOrigin.getAllFiles(f)
			var resList = new ArrayList[Document]
			var count = 0
			files.foreach(x => 
				try{
					val id = ImportOrigin.getwenshuID(x.getName)
					if (id != "")
						if (!dborigin2.find(eqq("_id", id)).iterator.hasNext)
							if (!dbnewdata.find(eqq("_id", id)).iterator.hasNext){
								var d = new Document
								println(x.getPath)
								d.append("_id", id)
								d.append("path", x.getPath)
								d.append("content", ImportOrigin.filterHtml(Source.fromFile(x, ImportOrigin.detector(x)).getLines().toArray).mkString("\n"))
								resList.add(d)
								count = count+1
								if (count == 10000) {
									dbnewdata.insertMany(resList, new InsertManyOptions().ordered(false))
									count = 0
									resList.clear
									log.warn("finish insert 10000:")
								}
							}
				} catch {
					case e: Throwable => log.error(e)
				}
			)
			try{
				dbnewdata.insertMany(resList, new InsertManyOptions().ordered(false))
				resList.clear()
			} catch {
				case e: Throwable => log.error(e)
				e.printStackTrace
			}
		})
	}
	
  def main(args: Array[String]): Unit = {

  }
}