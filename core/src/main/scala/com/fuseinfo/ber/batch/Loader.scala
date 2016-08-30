package com.fuseinfo.ber.batch

import com.fuseinfo.ber.external._
import com.fuseinfo.ber.conf._
import java.lang.reflect.Constructor
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
    val stdBaseName = berConf getValueByKey "std"
    val stdClassName = berConf getValueByKey "class.std"
    val stdBaseObject = Class.forName(stdBaseName).newInstance().asInstanceOf[StdClass[Row]]
    
    val stdFuncs = berConf getValuesByPrefix "std."
    val stdFuncObjs = stdFuncs.map(pair => {
      val eInd = pair._2.indexOf('=')
      val cInd = pair._2.indexOf(',', eInd)
      val className = if (cInd > 0) pair._2.substring(eInd+1, cInd) else pair._2.substring(eInd+1)
      val stdInstance = Class.forName(className).newInstance().asInstanceOf[StdClass[AnyRef]]
      if (cInd > 0) stdInstance.setup(pair._2.substring(cInd + 1))
      (pair._2.substring(0, eInd), stdInstance)
    });
    
    
 //   val stdRDD = entityDF.mapPartitions( iter => {
 //       stdBaseClass
 //     } 
 //   )
    
    
    //Step 5. persist STD_RDD to K/V store
    
    //Step 6. bucket STD_RDD to multiple BKT_RDD <Long, Set<Long>>
    
    //Step 7. persist BKT_RDD to K/V store
    
    //Step 8. Cross-match to create linkage <Long, Set<Long>>
    
    //Step 9. persist linkage
    
  }

}
