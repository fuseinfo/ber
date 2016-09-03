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

package com.fuseinfo.ber.batch

import com.fuseinfo.ber.external._
import com.fuseinfo.ber.conf._
import java.lang.reflect.{Field}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yang Li
 */
object Loader {
   
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("BerLoader")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    //Step 1. Load configurations
    val entity = conf get "entity"
    assert(entity != null)
    
    val berConf = new BerConf(entity, conf)
    
    //Step 2. Load flatten entities to RAW_DF
    val inputClass = berConf getValueByKey "inputClass"
    val inputData = berConf getValueByKey "inputData"
    val inputInstance = Class.forName(inputClass).asSubclass(classOf[InputClass]).newInstance()
    val entityDF = inputInstance.read(inputData, sqlContext)
    
    //Step 3. Optionally persist RAW_DF to K/V store
    
    //Step 4. call Standardization functions to transform to STD_RDD <Long, STD_Entity>
    val stdClassName = berConf getValueByKey "class.std"
    val stdClass = Class.forName(stdClassName)
    val fields = stdClass.getDeclaredFields
    val newMap = fields.map(field => field.getName -> field)(collection.breakOut): Map[String, Field]  

    val stdBaseName = berConf getValueByKey "std"
    val stdBaseInstance = Class.forName(stdBaseName).newInstance().asInstanceOf[StdClass]
    
    val stdFuncs = berConf getValuesByPrefix "std."
    val stdFuncObjs = stdFuncs.map(pair => {
      val eInd = pair._2.indexOf('=')
      val cInd = pair._2.indexOf(',', eInd)
      val className = if (cInd > 0) pair._2.substring(eInd+1, cInd) else pair._2.substring(eInd+1)
      val stdInstance = Class.forName(className).newInstance().asInstanceOf[StdClass]
      if (cInd > 0) stdInstance.setup(pair._2.substring(cInd + 1))
      (parseFieldName(pair._2.substring(0, eInd), newMap), stdInstance)
    })(collection.breakOut):List[((Option[Field], Option[String]), StdClass)];
    
    val stdRDD = entityDF.map( entity => {
      val stdBase = stdBaseInstance(entity)
      stdFuncObjs.foreach(f => {
        f._1 match {
          case (Some(field), options) => {
            val res = f._2.apply(entity)
            field.set(stdBase, res)
          }
          case (None, _) => {}
        }
      })
      stdBase
    }).zipWithUniqueId.map(f => (f._2, f._1))
    
    
    //Step 5. persist STD_RDD to K/V store
    
    //Step 6. bucket STD_RDD to multiple BKT_RDD <Long, Set<Long>>
    
    
    //Step 7. persist BKT_RDD to K/V store
    
    //Step 8. Cross-match to create linkage <Long, Set<Long>>
    
    //Step 9. persist linkage
    
  }
  
  def parseFieldName(fieldName: String, fieldMap: Map[String, Field]) = {
    val beginIndex = fieldName.indexOf('[')
    if (beginIndex > 0 && fieldName.endsWith("]")) {
      (fieldMap get fieldName.substring(0, beginIndex), Some(fieldName.substring(beginIndex + 1, fieldName.length - 1)))
    } else {
      (fieldMap get fieldName, None)
    } 
  }

}
