package org.gz.util

import com.typesafe.config.ConfigFactory

trait Conf {
  val config =  ConfigFactory.load("application.conf")
}

object ConfigUtil extends Conf{
	//get不到的时候初始化失败
	val (ftpuser, ftppasswd, fptURI) = (config.getString("ftp.user"), config.getString("ftp.passwd"), config.getString("ftp.uri"))
}