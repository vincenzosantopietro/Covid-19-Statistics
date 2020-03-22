package com.vinx.covid.statistics.metrics
import org.apache.spark.sql.DataFrame
/***
 * Generates a plot containing the number of deaths in china over time
 *
 * @param:
 *    - data DataFrame with data
 *    - outputPath path where to store the output image
 */
class ChinaDeathsOverTimeMetricsGenerator (data: DataFrame, outputPath: String) extends MetricsGenerator {
  override def generate: Unit = {
    // TODO: add logic here
    // TODO: Prepare data

    val plot = Vegas("Deaths in China")
  }
}
