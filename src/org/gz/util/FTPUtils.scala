package org.gz.util

import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPFile
import java.io.File
import java.io.FileInputStream
import org.apache.commons.net.ftp.FTP
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream

class FTPUtils(user: String, passwd: String, server: String) {
	
	def this() = {		
		this(ConfigUtil.ftpuser, ConfigUtil.ftppasswd, ConfigUtil.fptURI)
	}
	
	val ftp: FTPClient = new FTPClient
	ftp.connect(server)
	ftp.login(user, passwd)
	ftp.enterLocalPassiveMode()
	
		
	def mkdir(path: String) = {
		ftp.changeWorkingDirectory("/")
		ftp.makeDirectory(path)
	}
	
	def uploadFile(path: String, f: File, args: UploadArgs.Value = UploadArgs.normal) = {
		ftp.changeWorkingDirectory("/")
		ftp.changeWorkingDirectory(path)
		ftp.setFileType(FTP.BINARY_FILE_TYPE)		
		val res = args match {
			case UploadArgs.normal =>
				var flag = false 
				val files = ftp.listFiles()
				files.foreach{x => 
					if (x.getName == f.getName) flag = true
				}
				if (!flag) ftp.storeFile(f.getName, new BufferedInputStream(new FileInputStream(f))) else true 
			case UploadArgs.force => ftp.storeFile(f.getName, new BufferedInputStream(new FileInputStream(f))) 
		}		
		res
	}
		
	def uploadOkFile(path: String, fileName: String) = {
		ftp.changeWorkingDirectory("/")
		ftp.changeWorkingDirectory(path)
		ftp.setFileType(FTP.BINARY_FILE_TYPE)
		ftp.storeFile(fileName, new ByteArrayInputStream("ok".getBytes))		
	}
	
	def close = {
		ftp.disconnect
	}
}

object UploadArgs extends Enumeration{
	type UploadArgs = Value
	val normal, force = Value
}

object FTPUtils{
	def main(args: Array[String]): Unit = {
	  
	}
}

