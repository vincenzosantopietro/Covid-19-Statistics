package com.vinx.covid.statistics.metrics
import com.vinx.covid.statistics.metrics.utils.InfluxDBSink
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
/***
 * Generates a plot containing the number of deaths in china over time
 *
 * @param:
 *    - data DataFrame with data
 *    - outputPath path where to store the output image
 */
class ItalyDeclaredCasesOverTime(data: DataFrame, outputPath: String) extends MetricsGenerator {
  override def generate(): Unit = {
    new InfluxDBSink("http://localhost:8086/","covid-italy").
      storeMetrics(data)
  }
}
