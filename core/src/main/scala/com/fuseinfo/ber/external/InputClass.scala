package com.fuseinfo.ber.external

import org.apache.spark.sql.{SQLContext, DataFrame}

trait InputClass {
  def read(inputData: String, sqlContext: SQLContext) :DataFrame
}