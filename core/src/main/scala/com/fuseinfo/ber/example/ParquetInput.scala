package com.fuseinfo.ber.example

import com.fuseinfo.ber.external.InputClass
import org.apache.spark.sql.{SQLContext, DataFrame}

class ParquetInput extends InputClass {
  def read(inputData: String, sqlContext: SQLContext) = sqlContext.read.parquet(inputData)
}