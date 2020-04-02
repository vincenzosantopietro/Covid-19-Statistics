package com.vinx.covid.statistics.data

object DataParserFactory {
  val italyNationalCases : String = "italyDeclaredCasesOverTime"
  val italyCasesPerCities : String = "italyCasesPerCityOverTime"

  def createDataParser(metricsType : String, inputPath : String): DataParser ={
      metricsType match {
        case `italyNationalCases` => new ItalianCNMDataParser(inputPath)
        case `italyCasesPerCities` => new ItalianCitiesCNMDataParser(inputPath)
        case _ => throw new IllegalArgumentException(s"${metricsType} does not relate to a valid data parser")
      }
  }
}
