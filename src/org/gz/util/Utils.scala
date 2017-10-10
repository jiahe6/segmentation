package org.gz
package util

import java.net.InetAddress
import java.net.NetworkInterface
import scala.collection.JavaConversions._
import java.net.Inet4Address
import java.io.File
import java.io.InputStream
import java.io.FileInputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.InputStreamReader

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

  def main(args: Array[String]): Unit = {
    IOUtils.decompressZip(new File("D:/library/wenshu/20170612.rar"), "D:/library/wenshu/20170612/")
  }
}