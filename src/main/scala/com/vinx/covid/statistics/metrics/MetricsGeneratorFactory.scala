package com.vinx.covid.statistics.metrics

import org.apache.spark.sql.DataFrame

object MetricsGeneratorFactory {

  val chinaDeathsOverTimeMetricType : String = "chinaDeathsOverTime"
  //private val allowedMetrics = Set(chinaDeathsOverTimeMetricType)

  def createMetricsGenerator(metricType: String, data: DataFrame, outputPath: String): MetricsGenerator = {
    metricType match {
      case `chinaDeathsOverTimeMetricType` => new ChinaDeathsOverTimeMetricsGenerator(data, outputPath)
      case _ => throw new IllegalArgumentException(s"${metricType} is not a valid metric type")
    }
  }
}
