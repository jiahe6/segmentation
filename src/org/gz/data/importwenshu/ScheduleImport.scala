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

/**
 * 定期导入程序
 */
object ScheduleImport extends Conf{
	val log = LogManager.getLogger(this.getClass.getName())
	val sdf = new SimpleDateFormat("yyyyMMdd")
	
	val c = Calendar.getInstance
	c.setTime(sdf.parse(sdf.format(new Date(System.currentTimeMillis()))))
	val wenshuPath = if(System.getProperty("os.name").toLowerCase().startsWith("win")) config.getString("importwenshu.winrarpath") else config.getString("importwenshu.linuxrarpath")

	def getStartTime = config.getString("starttime")
	
	def DownloadRar(day: String) = {
		{new URL("http://wenshu.court.gov.cn/DownLoad/FileDownLoad.aspx?action=1&userName=dsjyjy&pwd=yjy201611&dates=" + day) #> new File("D:/library/wenshu/" + day) !!}
	}
	
	def unzipRar() = {
		
	}
	
	def insertOld() = {
    val runa = new Runnable(){
      override def run(): Unit = {
      	try {
      		val c2 = Calendar.getInstance
					c.setTime(sdf.parse(getStartTime))
      		if (c.after(c2))
      			DownloadRar(sdf.format(c.getTime))        		        	
        	c2.add(Calendar.DAY_OF_MONTH, 1)
      		log.info(s"Downloaded wenshu${sdf.format(c.getTime)} at " + new Date(System.currentTimeMillis))      		
      	} catch {
      		case e: Throwable => log.error(s"return error when downloading wenshu${sdf.format(c.getTime)} at " + new Date(System.currentTimeMillis))
      	}
      }
    }
		val scheduler = Executors.newScheduledThreadPool(1)
    val sf = scheduler.scheduleAtFixedRate(runa, 0, 180, TimeUnit.SECONDS)
  }


	
  def main(args: Array[String]): Unit = {
    val scheduler = Executors.newScheduledThreadPool(1)
  		val r = new Runnable(){
  			override def run(): Unit = {
  				
  			}
    }
  	//scheduler.schedule(r, 1, TimeUnit.DAYS)
    println(c.getTime)
  }
}