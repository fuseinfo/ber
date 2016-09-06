/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.fuseinfo.ber.conf

import java.sql.{Connection, DriverManager}
import org.apache.spark.SparkConf

class BerSqlConf(val conf: SparkConf) {
  private val confMap = scala.collection.mutable.Map[String, String]()
  val confDriver = conf get "ber.confDriver"
  val confUrl    = conf get "ber.confUrl"
  val confUser   = conf get "ber.confUser"
  val confPass   = conf get "ber.confPass"
  val confSql    = conf get "ber.confSql"
  var connection:Connection = _
  try {
    Class forName confDriver
    connection = DriverManager.getConnection(confUrl, confUser, confPass)
    val statement = connection.createStatement
    val rs = statement.executeQuery(confSql)
    while (rs.next) {
      confMap.put(rs getString 1, rs getString 2)
    }
  } catch {
    case e: Exception => e.printStackTrace
  } finally {
    connection.close 
  }
  
  
  def getValueByKey(key: String) = {
    confMap get key match {
      case Some(value) => value
      case None => ""
    }
  }
  
  def getValuesByPrefix(prefix: String) = {
    confMap.filter(kv => kv._1.startsWith(prefix)).toMap
  }
  
  def getValuesByGroup(group: String) = {
    val len = group.length + 1;
    getValuesByPrefix(group + ".").map(kv => kv._1.substring(len) -> kv._2)
  }
}