package org.gz
package util

import java.net.InetAddress
import java.net.NetworkInterface
import scala.collection.JavaConversions._
import java.net.Inet4Address

class Utils {
  
  def getHostID(host: String) = {
    val dbo = new DBOperation
    val rs = dbo.search(s"select * from host_info where ip = '$host'")
    val res = if (rs.next) rs.getInt("host_id") else 0
    dbo.close
    res
  }
  
  def getHostIP(hostID: Integer) = {
    val dbo = new DBOperation
    val rs = dbo.search(s"select * from host_info where host_id = '$hostID'")
    val res = if (rs.next) rs.getString("ip") else ""
    dbo.close    
    res
  }
  
  def warnExists(host_id: Integer, warnType: Int) = {
    val dbo = new DBOperation
    val rs = dbo.search(s"select * from system_log where host_id = $host_id and type = $warnType and solved = 0")
    val res = if (rs.next) true else false
    dbo.close
    res
  }
}

object Utils {
  private[gz] def getIpAddress = {
    val address = InetAddress.getLocalHost
    var res: String = address.getHostAddress
    if (address.isLoopbackAddress){
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
               !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            res = addr.getHostAddress
          }
        }
    }
    res
  }
  
  private[gz] def generateHostID(ip: String, port: String) = {
    s"$ip:$port"
  }
  
  private[gz] def pingHost(host: String) = {
    import scala.sys.process._
    val res = Seq("sh", "-c", s"ping -c 3 $host").!
    if (res==0) true else false
  }
  
  private[gz] def getWarnType(funcName: String) = {
    funcName match {
      case "getHostAvailabelSpace" => 0
      case "getAvailableSpace" => 1
      case "stateChange" => 2
      case _ => 3
    }
  }

  def main(args: Array[String]): Unit = {
    println(getIpAddress)
  }
}