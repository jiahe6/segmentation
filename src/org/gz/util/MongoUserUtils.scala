package org.gz.util

class MongoUserUtils extends Conf{
	//集群配置信息
  val clusterUser = config.getString("mongo.cluster.user")
  val clusterPW = config.getString("mongo.cluster.passwd")
  val clusterURI = "192.168.12.161:27017"
  val clusterAuthDB = config.getString("mongo.cluster.authDB")
  
  //备份单机配置信息
  val backupUser = config.getString("mongo.backup.user")
  val backupPW = config.getString("mongo.backup.passwd")
  val backupURI = "192.168.12.160:27017"
  val backupAuthDB = config.getString("mongo.backup.authDB") 
  
  def generateMongoURI(user: String, passwd: String, uri: String, authDB: String) = {
  	s"mongodb://${user}:${passwd}@${uri}/?authSource=${authDB}"
  }
  
  def clusterMongoURI = generateMongoURI(clusterUser, clusterPW, clusterURI, clusterAuthDB)
  
  def backupMongoURI = generateMongoURI(backupUser, backupPW, backupURI, backupAuthDB)
  
}