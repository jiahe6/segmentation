package org.gz.ftp

import org.gz.util.FTPUtils
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import org.gz.util.Conf
import java.io.File
import scala.util.Try
import org.apache.logging.log4j.LogManager
import org.gz.util.UploadArgs

object UploadFile extends Conf{
  
	val log = LogManager.getLogger(this.getClass.getName())
	val ftp = new FTPUtils
	val sdf = new SimpleDateFormat("yyyyMMdd")
	val rarPath = if (System.getProperty("os.name").toLowerCase().startsWith("win")) config.getString("importwenshu.winrarpath") else config.getString("importwenshu.linuxrarpath")
	val mode = config.getString("ftp.uploadmode") match {
		case "force" => UploadArgs.force
		case _ => UploadArgs.normal
	}
  
	/**
	 * 上传文件 没做异常检测
	 */
  def uploadFile(startTime: Date, endTime: Date): Boolean = {
		val startc = Calendar.getInstance
  	startc.setTime(sdf.parse(sdf.format(startTime)))  	
  	val endc = Calendar.getInstance
  	endc.setTime(sdf.parse(sdf.format(endTime)))
  	var res = false
  	while (!startc.after(endc)){
  		val timestr = sdf.format(startc.getTime)
  		try{
  			ftp.mkdir(timestr)
  			res = ftp.uploadFile(timestr, new File(s"${rarPath}/${timestr}.rar"), mode)
  			if (res) ftp.uploadOkFile(timestr, s"$timestr.rar.ok")
  		} catch {
  			case e: Throwable =>
  				log.error(s"upload to ftp fail: ${timestr}.rar")
  				log.error(e)
  		}
  		println(s"upload ${timestr}.rar")
  		startc.add(Calendar.DAY_OF_MONTH, 1)
  	}
		res
  }

	/**
	 * 上传文件 没做异常检测
	 */
	def uploadFile(time: Date): Boolean = {
		uploadFile(time, time)
	}
	
	/**
	 * 上传文件 没做异常检测
	 */
	def uploadFile(str: String): Boolean = {
		uploadFile(sdf.parse(str))
	}
	 
	def main(args: Array[String]): Unit = {
	  uploadFile("20170119")
	}
}