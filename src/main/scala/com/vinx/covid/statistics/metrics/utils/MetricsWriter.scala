package com.vinx.covid.statistics.metrics.utils

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer

trait MetricsWriter {
  def storeMetrics()
}
