package org.gz.util

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException

class DBOperation extends Conf {
  
  private val driver = "com.mysql.jdbc.Driver"
  private val url = config.getString("jdbc.url")
  private val username = config.getString("jdbc.user")
  private val password = config.getString("jdbc.passwd")
  private var connection:Connection = null
  private var res: ResultSet = null
  Class.forName(driver)
  connection = DriverManager.getConnection(url, username, password)
  
  /**
   * select
   */
  def search(sql: String)  = {
    try {
      val statement = connection.createStatement()
      res = statement.executeQuery(sql)
      res
    }
    catch {
      case e: Throwable => throw new SQLException(e.toString())
    }
  }
  
  /**
   * delete，insert，update
   */
  def update(sql: String) = {
    try {
      val statement = connection.createStatement()
      statement.execute(sql)
    }
    catch {
      case e: Throwable => throw new SQLException(e.toString())
    }
  }
  
  def close = {
    connection.close()
  }
}