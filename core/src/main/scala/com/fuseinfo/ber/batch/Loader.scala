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

import collection.JavaConversions._
import com.fuseinfo.ber.util.MathUtils
import com.fuseinfo.ber.standardizer.Standardizer
import gnu.trove.set.hash.TLongHashSet
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.{Map, Seq}
import scala.collection.mutable.ArrayBuffer

/**
 * @author Yang Li
 */
object Loader {
  def persistLevel = StorageLevel.MEMORY_AND_DISK_SER
  val emptyLongHashSet = new TLongHashSet

  def main(args : Array[String]) {
    val sparkConf = new SparkConf().setAppName("BerLoader")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val loader = new Loader
    val conf = loader.loadConf("BatchLoader.xml")
    loader.runMain(sqlContext, conf)
    sc.stop
  }
}
    
class Loader {
  def runMain(sqlContext: SQLContext, conf:Configuration) {
    //Step 1. Compile Configuration
    val maxBuckets = conf.get("ber.buckets.max","1000000").toInt
    
    //Load entities to RAW_DF
    val sqlText = conf.get("ber.loader.sql")
    val rawDF = readSource(sqlContext, sqlText)
    val fieldMap = rawDF.columns.zipWithIndex.toMap
    val pks = conf.get("ber.loader.pk")
    val pkeys = if (pks != null) pks.split(",").map(pk => fieldMap.get(pk.trim).get) else null
    
    //Step 2: Load and initialize Standardizers
    //Each standardizer will be assigned an internal type in Byte
    val standFuncMap = conf.getValByRegex("ber\\.standardizer\\.\\w+").map{case (pName,pVal) => 
      val nameInd = pName.substring(17)
      val dot = nameInd.indexOf('.')
      val pos = fieldMap.get(nameInd.substring(dot + 1)).get
      val comma = pVal.indexOf(',')
      val clazz = Class.forName(if (comma > 0) pVal.substring(0, comma) else pVal).asInstanceOf[Class[Standardizer[Any]]]
      val standardizer = if (comma < 0) clazz.newInstance
          else clazz.getConstructor(classOf[String]).newInstance(pVal.substring(comma + 1))
      (nameInd.substring(0, dot).toByte, (pos, standardizer))
    }
    
    //Step 3. call Standardization functions to transform to RDD[PK, (Type, Standardized)] and preserve RDD[PK, Row]
    val standardizedRowRdd = standardizeRow(rawDF, standFuncMap, pkeys).persist(Loader.persistLevel)
    val standardizedRdd = standardizedRowRdd.map{case ((map, row),pk) => (pk, map)}
    val rowRdd = standardizedRowRdd.map{case ((map, row),pk) => (pk, row)}

    //Step 4. Optionally persist RAW_DF to K/V store

    
    //Step 5: Create buckets by grouping hashes together
    // ((Type, Hash), PK)
    val hashededRdd = hashRdd(standardizedRdd).persist(Loader.persistLevel)
    // ((Type, Hash), Set<PK> or Number of PK)
    val bucketedRdd = bucketRdd(hashededRdd, maxBuckets)

    //Step 6. Optionally persist BKT_RDD to K/V store

    //Step 7: Join the buckets to Bucketed RDD
    // (PK, (Type, Bucket_Size, Related Rows))
    val joinedRdd = joinRdd(hashededRdd, bucketedRdd)
    
    //Step 8. Cross-match to create linkage [Long, Set[Long]]
    
    //Step 9. persist linkage
    
  }
  
  def loadConf(confName: String) = {
    val conf = new Configuration()
    conf.addResource(getClass.getClassLoader.getResource(confName))
    conf
  }
  
  /**
   *  @param sqlContext SQLContext
   *  @param sqlText The SQL statement to pull data. Each row should contain an entity record. 
   *                 If an entity may contain more than one attribute per type, 
   *                 we should use collection type for the attribute 
   *  @return DataFrame              
   */
  def readSource(sqlContext: SQLContext, sqlText: String) = sqlContext.sql(sqlText)
  
  /**
   *  @param df Input DataFrame
   *  @param standFuncMap Type -> (Position of the field in the DataFrame, Standardizer function)
   *  @param pkeys an array of primary keys. if no primary key was provided, a system generated primary key will be used.
   *               If there is only 1 primary key and the type is Long, we will use the primary key directly.
   *               In other cases, we will create a hash in Long as the primary key
   *  @return an RDD [((Map(Type -> Standardized Values), Row), Primary Key)]
   */
  def standardizeRow(df: DataFrame, standFuncMap:Map[Byte, (Int, Standardizer[Any])], pkeys: Array[Int]) =
    if (pkeys == null) df.map(row => 
      (standFuncMap.map{case (typ, (pos, standardizer)) => typ -> row.get(pos)}, row)).zipWithUniqueId
    else df.map{row => 
      val pks = pkeys.map(pk => row.get(pk))
      val index = if (pks.length == 1 && pks(0).isInstanceOf[Long]) pks(0).asInstanceOf[Long] 
      else {
        var hash = 0L
        pks.foreach { x => hash = hash*31 + MathUtils.murmurHash64A(x.toString, 0) }
        hash
      }
      ((standFuncMap.map{case (typ, (pos, standardizer)) => typ -> standardizer.apply(row.get(pos))}, row), index)
    }
  
  /**
   *  Create hash(es) for each attribute and flatten the result
   *  @param standardizedRdd standardized RDD[(Primary Key, Map[Type -> Value])]
   *  @return RDD[(Map[Type -> Hash], Primary Key)]
   */
  def hashRdd(standardizedRdd: RDD[(Long, Map[Byte, Any])]) =
    standardizedRdd.flatMap{case (pk, attrs) => 
      attrs.flatMap{
        case (typ, attr:Iterable[Any]) => 
          attr.map{
            case data:Long => ((typ, data), pk)
            case data => ((typ, stringHash(data.toString)), pk)
          }
        case (typ, attr:Long) => ((typ, attr), pk)::Nil
        case (typ, attr) => ((typ, stringHash(attr.toString)), pk)::Nil
      }
    }
  
  /**
   * Each type-hash is a bucket. Aggregate all primary keys that belong to a bucket together
   * @param hashedRdd  RDD[(Bucket, Primary Key)]
   * @param maxBuckets maximum size of a bucket before we decide to keep track of the number of rows only
   * @return RDD[Bucket -> TLongHashSet | Long]
   */
  def bucketRdd(hashedRdd: RDD[((Byte, Long), Long)], maxBuckets:Int) =
    hashedRdd.aggregateByKey((new TLongHashSet).asInstanceOf[Any])((agg, v) => 
      agg match {
        case sum: Long => sum + 1
        case set: TLongHashSet => 
          if (set.size >= maxBuckets) set.size + 1L else { set.add(v); set }
      }, (a1, a2) => 
      (a1, a2) match {
        case (a1: Long, a2: Long) => a1 + a2
        case (a1: Long, s2: TLongHashSet) => a1 + s2.size
        case (s1: TLongHashSet, a2: Long) => s1.size + a2
        case (s1: TLongHashSet, s2: TLongHashSet) => 
          if (s1.size + s2.size > maxBuckets) s1.size + s2.size else {s1.addAll(s2); s1}
    })
  
  /**
   * Join each row back to buckets to find all rows that share the same buckets
   * @param hashedRdd RDD[(Bucket, Primary Key)]
   * @param bucketedRdd RDD[Bucket -> TLongHashSet | Long]
   * @return RDD[(Primary Key, (Type, Size, TLongHashSet))]
   */
  def joinRdd(hashedRdd: RDD[((Byte, Long), Long)], bucketedRdd: RDD[((Byte, Long), Any)]) = 
    hashedRdd.join(bucketedRdd).map{case ((typ, hash), (pk, agg)) => 
      agg match {
        case set: TLongHashSet => {
          val newSet = new TLongHashSet
          val iter = set.iterator
          while (iter.hasNext) {
            val rel = iter.next
            if (rel < pk) newSet.add(rel)
          }
          (pk, (typ, set.size.toLong, newSet))
        }
        case sum: Long => (pk, (typ, sum, Loader.emptyLongHashSet))
      }

    }
  
  def stringHash(str: String) : Long = {
    MathUtils.murmurHash64A(str, 0)
  }
}
