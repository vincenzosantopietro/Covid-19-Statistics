package com.vinx.covid.statistics.metrics.utils

import org.apache.spark.sql.DataFrame

trait MetricsWriter {
  def storeMetrics(metrics: DataFrame)
}
