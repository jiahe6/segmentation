package org.gz.judgeresult

import scala.collection.mutable.HashMap

/**
 * 词典，及初分的正则
 */
object InitClassify {
	
  val dict = Array(
  		"撤销前审判决",
  		"定罪", "有期徒刑", "缓刑", "拘役", "管制", "假释", "剥夺政治权利",
  		"罚金", "退赔", "没收作案工具及赃款赃物", "扣押", "收缴", "追缴",
  		"不予减刑", "减刑", "准许撤回上诉", "准许撤回起诉", "驳回上诉，维持原判", 
  		"other"
  		)
  		
  val dictMap = HashMap(
  		"定罪" -> "犯.+罪",
  		"有期徒刑" -> "(判处|执行)有期徒刑|判处其有期徒刑",
  		"缓刑" -> "缓刑",
  		"拘役" -> "拘役",
  		"管制" -> "管制[^刀]",
  		"假释" -> "假释",
  		"剥夺政治权利" -> "剥夺政治权利",
  		"罚金" -> "罚金",
  		"退赔" -> "退赔",
  		"没收作案工具及赃款赃物" -> "没收",
  		"扣押" -> "扣押",
  		"收缴" -> "收缴",
  		"追缴" -> "追缴",
  		"减刑" -> "减刑|减去.*刑",
  		"不予减刑" -> "不予减刑",
  		"准许撤回上诉" -> "准许.*撤回上诉",
  		"准许撤回起诉" -> "准许.*撤回起诉",
  		"驳回上诉，维持原判" -> "驳回上诉[，,、；;]?维持原判|驳回.*上诉",
  		"撤销前审判决" -> "撤销"
  		)
  		
  def classify(str: String) = {
  	var flag = false
  	var result = "other"
  	var count = 0
  	while((!flag)&&(count < 19)){
  		dictMap.get(dict(count)) match {
  			case Some(regex) => 
  				val m = regex.r.findAllIn(str)
  				if (m.hasNext) {
  					flag = true
  					result = dict(count)
  				}
  			case None => 
  		}
  		count = count + 1
  	}
  	result
  }
  
  def main(args: Array[String]): Unit = {
    println(classify("二、管制具一把，依法予以没收"))
  }
}