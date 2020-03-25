package com.vinx.covid.statistics.data

import org.apache.spark.sql.DataFrame

trait DataParser {
  def getData: DataFrame
}
