package com.vinx.covid.statistics.metrics

import com.vinx.covid.statistics.data.{DataParserFactory, ItalianCNMDataParser}
import org.apache.spark.sql.DataFrame

object MetricsGeneratorFactory {

  val italyDeclaredCasesOverTimeMetricType : String = "italyDeclaredCasesOverTime"

  def createMetricsGenerator(metricType: String, dataPath : String, outputPath: String): MetricsGenerator = {
    metricType match {
      case `italyDeclaredCasesOverTimeMetricType` =>
        val data = DataParserFactory.createDataParser(metricType, "COVID-19/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv").getData
        new ItalyDeclaredCasesOverTime(data, outputPath)

      case _ => throw new IllegalArgumentException(s"${metricType} is not a valid metric type")
    }
  }
}
