package org.gz.util

import java.io.File
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import java.io.OutputStream
import org.apache.commons.compress.archivers.zip.ZipFile
import scala.collection.JavaConversions._
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.FileOutputStream

/**
 * 提供了创建文件夹路径和解压文件的功能，解压文件
 */
object IOUtils {
  def checkFileParent(file: File) = if (!file.getParentFile.exists) file.getParentFile.mkdirs
  
  def decompressZip(source: File, dest: String, sourceCharacters: String = "GBK", destCharacters: String = "UTF-8") = {
  	if (source.exists) {
  		var os: OutputStream = null
  		var inputStream: InputStreamReader = null
  		var outWriter: OutputStreamWriter = null
			val zipFile = new ZipFile(source, sourceCharacters)
			var entries = zipFile.getEntries
			
			entries.foreach(entry =>
				if (entry.isDirectory())
					new File(dest + entry.getName).mkdirs()					
				else if (entry != null) {
  				try{
  					val name = entry.getName
  					val path = dest + name
  					var content = new Array[Char](entry.getSize.toInt)  					
  					inputStream = new InputStreamReader(zipFile.getInputStream(entry), sourceCharacters)
  					println(inputStream.read(content))
          	val entryFile = new File(path)
  					checkFileParent(entryFile)
          	os = new FileOutputStream(entryFile)
  					outWriter = new OutputStreamWriter(os, destCharacters);
  					outWriter.write(new String(content))
  				} catch {
		  			case e: Throwable => e.printStackTrace()
		  		}finally{
						if (os != null){
							os.flush
							os.close
						}
						if (outWriter != null){
							outWriter.flush
							outWriter.close
						}
						if (inputStream != null) inputStream.close
					}
  			})
  		zipFile.close
  	}
  }
}