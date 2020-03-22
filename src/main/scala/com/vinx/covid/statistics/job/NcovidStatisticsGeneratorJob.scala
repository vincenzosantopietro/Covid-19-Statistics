package com.vinx.covid.statistics.job

import com.vinx.covid.statistics.data.{DataParser, MetricsCollector}
import com.vinx.covid.statistics.metrics.MetricsGeneratorFactory
import org.apache.spark.sql.DataFrame

object NcovidStatisticsGeneratorJob {

  def printSamples(data: DataFrame) : Unit = {
    println(s"Data Sample:")
    data.show(10)
    println("Data Schema:")
    data.schema.printTreeString()
  }

  def main(args: Array[String]): Unit = {
    // TODO: move this inside metrics estimators
    val data = new DataParser("2019_nCoV_data.csv").getData

    // Collect metrics to compute
    val metrics = MetricsCollector.collectMetrics("src/resources/config.json")

    for (metric <- metrics) {
      MetricsGeneratorFactory.createMetricsGenerator(metric, data, "output/" + metric + ".png")
    }
  }
}
