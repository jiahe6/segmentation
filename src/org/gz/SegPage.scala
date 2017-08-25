package org.gz

import scala.util.matching.Regex

case class SegPage(name:String, reg: Array[Regex], priority: Int) 