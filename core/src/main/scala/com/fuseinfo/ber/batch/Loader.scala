package com.fuseinfo.ber.batch

import org.apache.spark.sql.SQLContext
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
    
    //Step 2. Load flatten entities to RAW_RDD <Long, Entity>
    
    //Step 3. Optionally persist RAW_RDD to K/V store
    
    //Step 4. call Standardization functions to transform to STD_RDD <Long, STD_Entity>
    
    //Step 5. persist STD_RDD to K/V store
    
    //Step 6. bucket STD_RDD to multiple BKT_RDD <Long, Set<Long>>
    
    //Step 7. persist BKT_RDD to K/V store
    
    //Step 8. Cross-match to create linkage <Long, Set<Long>>
    
    //Step 9. persist linkage
    
  }

}
