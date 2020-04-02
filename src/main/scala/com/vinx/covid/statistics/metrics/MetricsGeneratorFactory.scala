package com.vinx.covid.statistics.metrics

import com.vinx.covid.statistics.data.{DataParserFactory, ItalianCNMDataParser}
import org.apache.spark.sql.DataFrame

object MetricsGeneratorFactory {

  val italyDeclaredCasesOverTimeMetricType : String = "italyDeclaredCasesOverTime"
  val italyCasesPerCityMetricType : String = "italyCasesPerCityOverTime"

  def createMetricsGenerator(metricType: String, dataPath: String): MetricsGenerator = {
    metricType match {
      case `italyDeclaredCasesOverTimeMetricType` =>
        val data = DataParserFactory.createDataParser(metricType, "COVID-19/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv").getData
        new ItalyDeclaredCasesOverTime(data)

      case `italyCasesPerCityMetricType`=>
        val data = DataParserFactory.createDataParser(metricType,"COVID-19/dati-province/dpc-covid19-ita-province.csv").getData
        new ItalyCasesPerCityOverTime(data)
      case _ => throw new IllegalArgumentException(s"${metricType} is not a valid metric type")
    }
  }
}
