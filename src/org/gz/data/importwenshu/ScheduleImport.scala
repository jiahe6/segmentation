package org.gz.data.importwenshu

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import java.io._
import java.net.URL
import sys.process._
import org.apache.logging.log4j.LogManager
import org.gz.util.Conf
import org.gz.util.IOUtils
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import org.gz.ImportOrigin
import org.bson.Document
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * 定期导入程序
 */
object ScheduleImport extends Conf{
	val log = LogManager.getLogger(this.getClass.getName())
	val sdf = new SimpleDateFormat("yyyyMMdd")
	lazy val mongoURI = new MongoClientURI("mongodb://gaoze:qazwsxedc!@192.168.12.161:27017/?authSource=admin")
	lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo.getDatabase("updatesdata")
	private lazy val dbColl = db.getCollection("newdata")
	val scheduler = Executors.newScheduledThreadPool(5)
	
	//计时器，插入旧数据时使用
	val c = Calendar.getInstance
	c.setTime(sdf.parse(sdf.format(new Date(System.currentTimeMillis()))))
	val c2 = Calendar.getInstance
	c2.setTime(sdf.parse(getStartTime))
	
	def getStartTime = config.getString("importwenshu.starttime")
	val (wenshuRarPath, wenshuPath) = 
		if(System.getProperty("os.name").toLowerCase().startsWith("win")) (config.getString("importwenshu.winrarpath"), config.getString("importwenshu.winwenshupath")) 
		else (config.getString("importwenshu.linuxrarpath"), config.getString("importwenshu.linuxwenshupath"))
	
	def DownloadRar(day: String) = {		
		{new URL("http://wenshu.court.gov.cn/DownLoad/FileDownLoad.aspx?action=1&userName=dsjyjy&pwd=yjy201611&dates=" + day) #> new File(wenshuRarPath + day + ".rar") !}
	}
	
	def unzipRar(day: String) = {
		val f = new File(wenshuRarPath + day + ".rar")
		if (f.exists) {
			log.info("start unzip file: " + wenshuRarPath + day + ".rar")
			IOUtils.decompressZip(f, wenshuPath + day + "/")
		} else {
			log.warn("cant find the rar file:\t" + wenshuRarPath + day)
		}
	}
	
	def insertOld() = {
    val runa = new Runnable(){
      override def run(): Unit = {
				if (c.after(c2))
     			doInsertByTime(c2)
      }
    }		
    val sf = scheduler.scheduleAtFixedRate(runa, 0, 180, TimeUnit.SECONDS)
    sf
  }
	
  def doInsertByTime(cal: Calendar) = {
  	println(sdf.format(cal.getTime))
  	try{
  		//如果zip包已经存在，那么啥也不干
  		val f = new File(wenshuRarPath + sdf.format(cal.getTime) + ".rar")
  		assert(!f.exists(), "压缩包已存在，不进行下面操作")
	  	//下载zip包
	  	DownloadRar(sdf.format(cal.getTime))        		        	
	 		log.info(s"Downloaded wenshu${sdf.format(cal.getTime)} at " + new Date(System.currentTimeMillis))
	 		Thread.sleep(5000)	 		
	 		//解压zip包
	  	unzipRar(sdf.format(cal.getTime))
	  	log.info("unziped wenshu")
	  	//写到芒果里
	  	ImportOrigin.folderToDocuments(new File(wenshuPath + sdf.format(cal.getTime) + "/"), dbColl)	  	
  	} catch {
  		case e: Throwable =>
  			log.error(s"error while processing ${wenshuRarPath}${sdf.format(cal.getTime)}.rar")
  			log.error(e)
  			e.printStackTrace
  	} finally {
  		//+1s
	 		cal.add(Calendar.DAY_OF_MONTH, 1)
  	}
  }
   	
  def main(args: Array[String]): Unit = {
  	val so = insertOld  	
  	//按天进行更新工作
  	val cn = Calendar.getInstance  	
		cn.setTime(sdf.parse(sdf.format(new Date(System.currentTimeMillis()))))
		cn.add(Calendar.DAY_OF_MONTH, -1)
		//按周进行备份工作,由于cn是前一天的计时器，所以在周日执行备份的话，需要把定时器设为前一天，即周6
		val cw = Calendar.getInstance  	
		cw.setTime(sdf.parse("20171007"))
		while (cn.after(cw)){
			cw.add(Calendar.WEEK_OF_MONTH, 1)
		}
		val r = new Runnable(){
			override def run(): Unit = {
				if (!c.after(c2)) so.cancel(false)
				//calendar+1在doinsertbytime里
  			doInsertByTime(cn)
  			//TODO： 进行数据处理
  			//TODO： 处理完毕后插入到origin2和forsearch中    			
  			//TODO： 要确保插入完了进行备份，所以单线程执行备份
  			if (cw.equals(cn)) {
  				ScheduleBackup.doBackUp(cw)
  				cw.add(Calendar.WEEK_OF_MONTH, 1)
  			}
			}
  	}
  	println("start")
  	scheduler.scheduleAtFixedRate(r, 0, 1, TimeUnit.DAYS)
  }
}