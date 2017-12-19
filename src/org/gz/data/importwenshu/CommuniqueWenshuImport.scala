package org.gz.data.importwenshu

import scala.io.Source
import org.gz.util.JsonParser

object CommuniqueWenshuImport {	
	
	def jsonToMongo(json: String) = {
		val jparser = new JsonParser
		jparser.setJson(json)
		
		val segdataj = jparser.getArrayByDot("data.fullJudgement.paragraphs")
		
	}
	
  def main(args: Array[String]): Unit = {
  	Source.fromFile("C:/Users/cloud/Desktop/类案搜索/公报前4条结果.txt").getLines().foreach(jsonToMongo)
  }
}