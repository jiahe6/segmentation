package org.gz.test

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

object TestOthers {
  def main(args: Array[String]): Unit = {
    val ss = ArrayBuffer[String]("1", "2")
    val s2 = ss
    s2 += "3"
    val hm = HashMap[String, Int]()
    hm += (("1", 1))
    hm += (("1", 2))
    println(hm.getOrElse("1", 0))
  }
}