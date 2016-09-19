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
import com.fuseinfo.ber.util._
import java.lang.reflect.{Field}
import java.io.ByteArrayOutputStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.{Map, Seq}
import scala.collection.mutable.ArrayBuffer
import com.fuseinfo.ber.util.MathUtils

/**
 * @author Yang Li
 */
object Loader {
   
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("BerLoader")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val newLevel = StorageLevel.MEMORY_AND_DISK_SER
    
    //Step 1. Load configurations
    val entityProps = loadConf(conf)
    
    //Step 2. Load flatten entities to RAW_DF
    val entityDF = loadRawData(entityProps, sqlContext)
    val columnMap = (entityDF.columns zip entityDF.columns.indices).toMap
    
    //Step 3. Optionally persist RAW_DF to K/V store
    
    //Step 4. call Standardization functions to transform to STD_RDD <Long, STD_Entity>
    // class.std refers to the class name of a standardized entity
    // We will understand all the fields defined in the class
    // fieldMap stores the list of 
    val stdClassName = entityProps.get("class.std") match {
      case Some(value) => value
      case None => throw new Exception("class.std must be set")
    }
    val declaredFields = Class.forName(stdClassName).getDeclaredFields
    val fieldMap = declaredFields.map(field => field.getName -> field)(collection.breakOut): Map[String, Field]  

    // base std function maps Row data to standardized entity
    val stdBaseName = entityProps.get("std") match {
      case Some(value) => value
      case None => throw new Exception("std must be set")
    }
    val stdBaseInstance = Class.forName(stdBaseName).newInstance().asInstanceOf[StdClass[Row]]
    
    //   key will be in the format: std.<to_field>
    // value will be in the format: <from_field>,<std func>[,parameter list]
    // an item in a List field can be denoted as: field[n]
    // an item in a Map field can be denoted as: field[key]
    // we can use field[] to invoke the standardization function for each item
    val stdFuncs = entityProps.filter(kv => kv._1.startsWith("std.")).map(kv => kv._1.substring(4) -> kv._2)
    
    // prepare a list of standardization functions in stdFuncObjs as
    // (From_Column_Name, Optional Index, To_Field, Standardization_Instance
    val stdFuncObjs = for {pair <- stdFuncs
         if fieldMap.contains(pair._1)
      }
      yield {
        val beginIndex = pair._2.indexOf(',')
        val col = parseColumnName(pair._2.substring(0, beginIndex))
        val endIndex = pair._2.indexOf(',', beginIndex + 1)
        val stdClassName = {
          if (endIndex > 0) pair._2.substring(beginIndex + 1, endIndex)
          else pair._2.substring(beginIndex + 1)
        }
        val stdInstance = Class.forName(stdClassName).newInstance().asInstanceOf[StdClass[Any]]
        if (endIndex > 0) stdInstance.setup(pair._2.substring(endIndex + 1))
        (col._1, col._2, fieldMap.get(pair._1).get, stdInstance)
      }
    
    val stdRDD = entityDF.map( row => {
      val stdBase = stdBaseInstance(row)
      stdFuncObjs.foreach(func => {
        func._2 match {
          case Some("") => {
              val columnInd = columnMap.get(func._1) match {
                case Some(idx) => {
                  val fieldValue = func._3.get(stdBase)
                  val columnValue = row.get(idx)
                  if (columnValue.isInstanceOf[Seq[Any]] && fieldValue.isInstanceOf[scala.collection.mutable.Buffer[Any]]) {
                    columnValue.asInstanceOf[Seq[Any]].foreach { 
                      item => fieldValue.asInstanceOf[scala.collection.mutable.Buffer[Any]].append(func._4.apply(item))  
                    }
                  } else if (columnValue.isInstanceOf[Map[String, Any]] && fieldValue.isInstanceOf[scala.collection.mutable.Map[String, AnyRef]]) {
                    columnValue.asInstanceOf[Map[String, Any]].foreach {
                      kv => fieldValue.asInstanceOf[scala.collection.mutable.Map[String, Any]].put(kv._1, func._4.apply(kv._2))
                    }
                  }
                }
                case None =>
              }                
          }
          case _ => {
            getColumnValue(row, columnMap, func._1, func._2) match {
              case Some(value) => {
                val result = func._4.apply(value)
                func._2 match {
                  case Some(option) => {
                    val fieldValue = func._3.get(stdBase)
                    if (fieldValue.isInstanceOf[scala.collection.mutable.Buffer[AnyRef]]) {
                      fieldValue.asInstanceOf[scala.collection.mutable.Buffer[AnyRef]].append(result)
                    } else if (fieldValue.isInstanceOf[scala.collection.mutable.Map[String, AnyRef]]) {
                      fieldValue.asInstanceOf[scala.collection.mutable.Map[String, AnyRef]].update(option, result)
                    } else {
                      func._3.set(stdBase, result)
                    }
                  }
                  case None => {
                    func._3.set(stdBase, result)
                  }
                }
              }
              case None =>
            }
          }
        }
      })
      stdBase
    }).zipWithUniqueId.map(f => (f._2, f._1)).persist(newLevel)
    
    
    //Step 5. persist STD_RDD to K/V store
    
    //Step 6. bucket STD_RDD to multiple BKT_RDD <Long, Set<Long>>
    val bktFuncs = entityProps.filter(kv => kv._1.startsWith("bkt.")).values
    val bktRDDs = bktFuncs.map(bktFunc => {
      val fieldDefs = bktFunc.split(";").map(fieldName => {
        val idx = fieldName.indexOf('.')
        if (idx > 0) {
          if (fieldName.charAt(idx - 1) == ']') {
            val beginIdx = fieldName.indexOf('[')
            val field = fieldMap.get(fieldName.substring(0, beginIdx)).get
            val subField = field.getClass.getField(fieldName.substring(idx + 1))
            (field, fieldName.substring(beginIdx + 1, idx - 1), subField)
          } else {
            val field = fieldMap.get(fieldName.substring(0, idx)).get
            val subFields = fieldName.substring(idx + 2, fieldName.length - 1).split("\\,").map(subField => field.getClass.getDeclaredField(subField))
            (field, subFields)
          }
        } else {
          fieldMap.get(fieldName).get
        }
      })
      stdRDD.flatMap(stdObj => {
        val baos = new ByteArrayOutputStream
        for (fieldDef <- fieldDefs) {
          fieldDef match {
            case (field: Field, key: String, subField: Field) => {
              
            }
            case (field: Field, subFields: Array[Field]) => {
              
            }
            case field:Field => {
              val fieldValue = field.get(stdObj)
              if (fieldValue == null) {
                return Array()
              }
              baos.write(field.get(stdObj).toString().getBytes)
            }
          }
        }
        new Array[Tuple2[Long, Long]](0)
      })
    })

    
    //Step 7. persist BKT_RDD to K/V store
    
    //Step 8. Cross-match to create linkage <Long, Set<Long>>
    
    //Step 9. persist linkage
    
  }
  
  def hash(bytes: Array[Byte]) : Long = {
    MathUtils.murmurHash64A(bytes, bytes.length, 0)
  }
  
  def hash(base: Array[Byte], bytes: Array[Byte]) : Long = {
    val both = new Array[Byte](base.length + bytes.length)
    System.arraycopy(base, 0, both, 0, base.length)
    System.arraycopy(bytes, 0, both, base.length, bytes.length)
    hash(both)
  }
  
  /*
   * Given a fieldDef and object, return a TraversableOnce object of hash numbers
   */
  def calcHash(fieldDefs:Array[AnyRef], stdObj:(Long, AnyRef)): TraversableOnce[Long] = {
    val baos = new ByteArrayOutputStream

    for (fieldDef <- fieldDefs) {
      fieldDef match {
        // a single value from a map
        case (field: Field, key: String, subField: Field) => {
          val fieldValue = field.get(stdObj)
          if (fieldValue == null || !fieldValue.isInstanceOf[Map[String, AnyRef]]) {
            return new Array[Long](0)
          }
          val subValue = fieldValue.asInstanceOf[Map[String, AnyRef]].get(key) match {
            case Some(subObj) => subField.get(subObj)
            case None => return new Array[Long](0)
          }
          if (subValue == null) {
            return new Array[Long](0)
          }
          baos.write(subValue.toString.getBytes)
        }
        // multiple values from a child list
        case (field: Field, subFields: Array[Field]) => {
          val fieldValues = field.get(stdObj)
          if (fieldValues == null || !fieldValues.isInstanceOf[scala.collection.mutable.Buffer[AnyRef]]) {
            return new Array[Long](0)
          }
          val fieldValArray = fieldValues.asInstanceOf[scala.collection.mutable.Buffer[AnyRef]]
          baos.flush
          val base = baos.toByteArray
          return fieldValArray.flatMap( fieldVal => {
            try {
              val baos2 = new ByteArrayOutputStream
              for (subField <- subFields) {
                val subVal = subField.get(fieldVal)
                baos2.write(subVal.toString().getBytes)
              }
              baos2.flush()
              Some(hash(base, baos2.toByteArray))
            } catch {
              case e: Exception => None
            }            
          })
        }
        // single value
        case field:Field => {
          val fieldValue = field.get(stdObj)
          if (fieldValue == null) {
            return new Array[Long](0)
          }
          baos.write(fieldValue.toString.getBytes)
        }
      }
    }
    baos.flush
    Array[Long](hash(baos.toByteArray))
  }

  def parseColumnName(columnName: String) = {
    val beginIndex = columnName.indexOf('[')
    if (beginIndex > 0 && columnName.endsWith("]")) {
      (columnName.substring(0, beginIndex), Some(columnName.substring(beginIndex + 1, columnName.length - 1)))
    } else {
      (columnName, None)
    }

  }
  
  def parseFieldName(fieldName: String, fieldMap: Map[String, Field]) = {
    val beginIndex = fieldName.indexOf('[')
    if (beginIndex > 0 && fieldName.endsWith("]")) {
      (fieldMap get fieldName.substring(0, beginIndex), Some(fieldName.substring(beginIndex + 1, fieldName.length - 1)))
    } else {
      (fieldMap get fieldName, None)
    } 
  }
  
  def getColumnValue(row: Row, columnMap: Map[String, Int], column: String, item: Option[String]) = {
    columnMap.get(column) match {
      case Some(index: Int) => {
        val value= row.get(index)
        item match {
          case Some(option) => {
            if (value.isInstanceOf[Seq[AnyRef]]) {
              Some(value.asInstanceOf[Seq[AnyRef]](Integer.parseInt(option)))
            } else if (value.isInstanceOf[Map[AnyRef, AnyRef]]) {
              Some(value.asInstanceOf[Map[String, AnyRef]] get option)
            } else {
              Some(value)
            }
          }
          case None => Some(value)
        }
      }
      case None => None
    }
  }
  
  def loadConf(conf: SparkConf) = {
    val entity = conf get "entity"
    assert(entity != null)
    val berConf = new BerSqlConf(conf)
    berConf.getValuesByGroup(entity);
  }
  
  def loadRawData(entityProps: Map[String, String], sqlContext: SQLContext) = {
    val inputClassOpt = entityProps.get("inputClass")
    val inputDataOpt = entityProps.get("inputData")
    (inputClassOpt, inputDataOpt) match {
      case (Some(inputClass), Some(inputData)) => 
        Class.forName(inputClass).asSubclass(classOf[InputClass]).newInstance().read(inputData, sqlContext)
      case _ => throw new Exception("inputClass and inputData can not be empty")
    }
  }
}
