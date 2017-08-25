package org.gz

import java.util.Calendar
import java.text.SimpleDateFormat
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.sys.process._

/**
 * @author cloud
 * 根据日志找到没跑完的数据，并将其移动到一个新的文件夹内：
 *  日志中存在具体路径，通过hashSet找到没出现过的日期，将其放入新的文件夹
 */
object FixData {
  def main(args: Array[String]): Unit = {
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
}