package org.gz.util

import com.fasterxml.jackson.databind.ObjectMapper
import scala.io.Source
import org.gz.test.TestJsonStruct
import scala.collection.JavaConversions._
import com.fasterxml.jackson.databind.JsonNode

class JsonParser {
  val objectMapper = new ObjectMapper
  val reader = objectMapper.reader()
  var json = ""
  
  def setJson(str: String) = {
  	json = str
  }
  
  def readClass[T](claz: Class[T]): T = {
  	objectMapper.readValue(json, claz)
  }
  
  def readClass[T](jsonStr: String, claz: Class[T]): T = {
  	objectMapper.readValue(jsonStr, claz)
  }
  
  def objectReader = reader.readTree(json)	
    
  def getArrayByDot(str: String) = {
  	val strs = str.split("\\.")
  	var res = objectReader
  	strs.foreach {x => res = res.get(x)}
  	res.iterator().toArray
  }
  
  def getByDot(str: String) = {
  	val strs = str.split("\\.")
  	var res = objectReader
  	strs.foreach {x => res = res.get(x)}
  	res
  }
  
}

object JsonParser {
	
	def getArrayByDot(jsonNode: JsonNode, str: String) = {
		val strs = str.split("\\.")
  	var res = jsonNode
  	strs.foreach {x => res = res.get(x)}
  	res.iterator().toArray
	}
	
	def getByDot(jsonNode: JsonNode, str: String) = {
  	val strs = str.split("\\.")
  	var res = jsonNode
  	strs.foreach {x => res = res.get(x)}
  	res
  }
	
	def main(args: Array[String]): Unit = {
	  val objectMapper = new ObjectMapper
	  val reader = objectMapper.reader()
	  val iter = Source.fromFile("C:/Users/cloud/Desktop/类案搜索/公报前4条结果.txt").getLines()
	  val str = iter.next.replace(65279.toChar.toString(), "")
	  println(str)
	  val res = reader.readTree(str)
	  val str2 = "{\"result\":{\"code\":0,\"message\":\"成功\"}}"
	  val jparser = new JsonParser
	  jparser.setJson(str)
	  //println(jparser.readClass(classOf[TestJsonStruct]).toString())
	  jparser.getArrayByDot("data.fullJudgement.paragraphs").foreach {println}
	  //println(objectMapper.readValue(str2, classOf[TestJsonStruct]).getResult)	  
	}
}