package com.vinx.covid.statistics.job

import com.vinx.covid.statistics.data.{ItalianCNMDataParser, MetricsCollector}
import com.vinx.covid.statistics.metrics.MetricsGeneratorFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

/***
 * @author Vincenzo Santopietro <vinsantopietro@gmail.com>
 */

object NcovidStatisticsGeneratorJob {

  private val dataPath = "src/main/resources/"

  def printSamples(data: DataFrame) : Unit = {
    println(s"Data Sample:")
    data.show(10)
    println("Data Schema:")
    data.schema.printTreeString()
  }

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org")
    logger.setLevel(Level.INFO)

    // Collect metrics to compute
    val metrics = MetricsCollector.collectMetrics("src/main/resources/config.json")

    logger.info("Metrics computation started!")
    for (metric <- metrics) {
      val metric_generator = MetricsGeneratorFactory.createMetricsGenerator(metric, dataPath)
      metric_generator.generate()
    }
    logger.info("Metrics computation successfully finished.")
  }
}
