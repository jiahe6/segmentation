package org.gz.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
//the standard URI is format like: mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
class MongoUserUtils extends Conf{
	//集群配置信息
  val clusterUser = config.getString("mongo.cluster.user")
  val clusterPW = config.getString("mongo.cluster.passwd")
  val clusterURI = config.getString("mongodb.clusteruri")
  val clusterAuthDB = config.getString("mongo.cluster.authDB")
  val clusterURIAll = config.getString("mongodb.clusteruriall")
  
  //备份单机配置信息
  val backupUser = config.getString("mongo.backup.user")
  val backupPW = config.getString("mongo.backup.passwd")
  val backupURI = config.getString("mongodb.backupuri")
  val backupAuthDB = config.getString("mongo.backup.authDB") 
  
  def generateMongoURI(user: String, passwd: String, uri: String, authDB: String) = {
  	s"mongodb://${user}:${passwd}@${uri}/?authSource=${authDB}"
  }
  
  def clusterMongoURI = generateMongoURI(clusterUser, clusterPW, clusterURI, clusterAuthDB)
  def clusterLocalMongoURI(localURI: String) = generateMongoURI(clusterUser, clusterPW, localURI, clusterAuthDB)
  
  def backupMongoURI = generateMongoURI(backupUser, backupPW, backupURI, backupAuthDB)
  
  def origin2URI = s"mongodb://${clusterUser}:${clusterPW}@${clusterURIAll}/wenshu.origin2?authSource=${clusterAuthDB}"
  
  def backupDefaultURI = s"mongodb://${backupUser}:${backupPW}@${backupURI}/wenshu.backup?authSource=${backupAuthDB}"
  
  def customizeSparkClusterURI(dbcoll: String) = s"mongodb://${clusterUser}:${clusterPW}@${clusterURIAll}/${dbcoll}?authSource=${clusterAuthDB}"
  
  def sparkSessionBuilder(inputuri: String = "", outputuri: String = "", jarName: String = "DataMigration.jar", extJars: Array[String] = Array()) = {
  	val inp = if (inputuri == "") origin2URI else inputuri
  	val oup = if (outputuri == "") backupDefaultURI else outputuri
  	SparkSession.builder()
//			.master("local")
			.master(sparkURI)
			.config(new SparkConf().setJars(Array(s"${hdfsURI}/mongolib/mongo-spark-connector_2.11-2.0.0.jar",
					s"${hdfsURI}/mongolib/bson-3.4.2.jar",
					s"${hdfsURI}/mongolib/mongo-java-driver-3.4.2.jar",
					s"${hdfsURI}/mongolib/mongodb-driver-3.4.2.jar",
					s"${hdfsURI}/mongolib/mongodb-driver-core-3.4.2.jar",
					s"${hdfsURI}/mongolib/commons-io-2.5.jar",
					s"${hdfsURI}/mongolib/config-1.2.1.jar",
					s"${hdfsURI}/${jarName}") ++ extJars))  	  
			.config("spark.cores.max", 80)		
			.config("spark.executor.cores", 16)
			.config("spark.executor.memory", "32g")
			.config("spark.mongodb.input.uri", inp)
			.config("spark.mongodb.output.uri", oup)
			.config("spark.mongodb.input.partitionerOptions.samplesPerPartition", 1)
			.getOrCreate()
  }
  
}