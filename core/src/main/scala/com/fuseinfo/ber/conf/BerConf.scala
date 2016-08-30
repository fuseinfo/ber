package com.fuseinfo.ber.conf

import java.sql.{Connection,DriverManager}
import org.apache.spark.{SparkConf, SparkContext}

class BerConf(val entity: String, val conf: SparkConf) {
  private val confMap = scala.collection.mutable.Map[String, String]()
  val confDriver = conf get "ber.confDriver"
  val confUrl    = conf get "ber.confUrl"
  val confUser   = conf get "ber.confUser"
  val confPass   = conf get "ber.confPass"
  val confSql    = conf get "ber.confSql"
  try {
    Class forName confDriver
    val connection = DriverManager.getConnection(confUrl, confUser, confPass)
    val statement = connection.createStatement
    val rs = statement.executeQuery(confSql)
    while (rs.next) {
      confMap.put(rs getString 1, rs getString 2)
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  
  def getValueByKey(key: String) = {
    confMap get key match {
      case Some(value) => value
      case None => ""
    }
  }
  
  def getValuesByPrefix(prefix: String) = {
    confMap.filter(kv => kv._1.startsWith(prefix))
  }
}