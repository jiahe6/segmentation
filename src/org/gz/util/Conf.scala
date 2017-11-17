package org.gz.util

import com.typesafe.config.ConfigFactory

trait Conf {
  val config =  ConfigFactory.load("application.conf")
  def sparkURI = "spark://" + config.getString("spark.uri")
  def hdfsURI = "hdfs://" + config.getString("hdfs.uri") + "/"
}

object ConfigUtil extends Conf{
	//get不到的时候初始化失败
	val (ftpuser, ftppasswd, fptURI) = (config.getString("ftp.user"), config.getString("ftp.passwd"), config.getString("ftp.uri"))
}