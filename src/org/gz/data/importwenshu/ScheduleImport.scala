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
import org.apache.commons.compress.archivers.zip.ZipFile
import sun.security.jca.GetInstance
import org.gz.util.MongoUserUtils
import org.gz.ftp.UploadFile

/**
 * 定期导入程序
 */
object ScheduleImport extends Conf{
	val log = LogManager.getLogger(this.getClass.getName())
	val sdf = new SimpleDateFormat("yyyyMMdd")
	lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
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
		
	def testDirPath(str: String) = {
		val f = new File(str)	
		IOUtils.checkFileParent(f)
		if (!f.exists()) f.mkdirs()
	}
	testDirPath(wenshuRarPath)
	testDirPath(wenshuPath)
	
	
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
				if ((c.after(c2))&&(!new File(wenshuRarPath + sdf.format(c2.getTime) + ".rar").exists()))					
     			doInsertByTime(c2)
      }
    }		
    val sf = scheduler.scheduleAtFixedRate(runa, 0, 180, TimeUnit.SECONDS)
    sf
  }
	
	private def doInsert(cal: Calendar) = {
		//下载zip包
		DownloadRar(sdf.format(cal.getTime))        		        	
	 	log.info(s"Downloaded wenshu${sdf.format(cal.getTime)} at " + new Date(System.currentTimeMillis))
	 	Thread.sleep(5000)	 		
	 	//解压zip包
	  unzipRar(sdf.format(cal.getTime))
	  log.info("unziped wenshu")
	  //传到FTP
	  UploadFile.uploadFile(cal.getTime)
	  //写到芒果里
	  ImportOrigin.folderToDocuments(
	  		new File(wenshuPath + sdf.format(cal.getTime) + "/"), 
	  		dbColl, 
	  		mongo.getDatabase("updatesdata").getCollection("processeddata"),
	  		mongo.getDatabase("wenshu").getCollection("origin2"),
	  		mongo.getDatabase("forsearch").getCollection("updatesdata")	  		
	  		)
	}
	
	/**
	 * 修复下载错误的Zip包
	 * 需要隔一段时间定时运行一下，毕竟那个验证码出来了即使重试了3词依旧会有错误包
	 */
	def fixErrorZip = {
		//打不开的rar
		val cal = Calendar.getInstance
		cal.setTime(sdf.parse("20140101"))
		val c2 = Calendar.getInstance
		c2.setTime(sdf.parse("20151231"))
		while (cal.before(c2)){
			try{
				val path = wenshuRarPath + s"${sdf.format(cal.getTime)}.rar"
				val zipFile = new ZipFile(new File(path), "GBK")
				zipFile.close()
			}catch {
				case e: java.util.zip.ZipException =>
					doInsertByTime(cal)
					e.printStackTrace()
				case e2: Throwable => e2.printStackTrace
			}
			cal.add(Calendar.DAY_OF_MONTH, 1)
		}
	}
	
	/**
	 * 对没经过处理的updatesdata.newdata数据进行处理操作
	 */
	def fixUnProcessData = {				
		val dbprocesseddata = mongo.getDatabase("updatesdata").getCollection("processeddata")
		val iter = dbColl.find().noCursorTimeout(true)
		iter.foreach(x => {			
			try{
				val res = ImportDataProcess.processData(x)
				dbprocesseddata.insertOne(res)
			}catch {
				case e: Throwable => log.error(e)
			}
		})
		val dbfixdata = mongo.getDatabase("updatesdata").getCollection("fixdata")
		val fixiter = dbfixdata.find().noCursorTimeout(true)
		fixiter.foreach(x => {
			try{
				val res = ImportDataProcess.processData(x)
				dbprocesseddata.insertOne(res)
			}catch {
				case e: Throwable => log.error(e)
			}
		})
		mongo.close
	}
	
  def doInsertByTime(cal: Calendar) = {
  	println(sdf.format(cal.getTime))
  	var attempt = 0
  	var flag = false  	
  	//如果zip包已经存在，那么啥也不干
	  while (!flag){
	  	try{
	  		attempt = attempt + 1	 
	  		//插入数据
		  	doInsert(cal)
	  		//执行成功了直接结束
		  	flag = true
	  	} catch {
	  		case e: java.util.zip.ZipException =>
	  			log.error(s"error while processing ${wenshuRarPath}${sdf.format(cal.getTime)}.rar")
	  			log.error(e)
	  			//最高法数据有问题，先睡个1分钟，再重试，重试次数多了就算了
	  			if (attempt < 4) Thread.sleep(60000) else {
	  				flag = true
	  				log.error(s"Error after 4 attempts at ${sdf.format(cal.getTime)}, check the wenshu.court.gov.cn source to know if the origin zip pack is null")
	  			}		  			
	  		case e: Throwable =>
	  			log.error(s"error while processing ${wenshuRarPath}${sdf.format(cal.getTime)}.rar")
	  			log.error(e)
	  			e.printStackTrace
	  	} 
  	}
  	//+1s
		cal.add(Calendar.DAY_OF_MONTH, 1)
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
				try{
					if (!c.after(c2)) so.cancel(false)
					//calendar+1在doinsertbytime里
					val f = new File(wenshuRarPath + sdf.format(cn.getTime) + ".rar")
					if (!f.exists())
	  				doInsertByTime(cn)
	  			else
	  				log.warn("file exist!")
	  			//TODO  数据处理已加入，但是没测试，现在进行到插入processeddata，如果测试成功则进行下一步
	  			//TODO  处理完毕后插入到origin2和forsearch中 ,完成			
	  			//TODO  要确保插入完了进行备份，所以单线程执行备份,完成
	  			if (cw.equals(cn)) {
	  				try{
	  					ScheduleBackup.doBackUp(cw)
	  				}catch{
	  					case e: Throwable => log.error(e)
	  				}	  				
	  				cw.add(Calendar.WEEK_OF_MONTH, 1)
	  			}
				}catch{
					case e: Throwable => log.error(e)
				}
			}
  	}
		Thread.sleep(5000)
  	println("start")
  	scheduler.scheduleAtFixedRate(r, 0, 1, TimeUnit.DAYS)
  }
}