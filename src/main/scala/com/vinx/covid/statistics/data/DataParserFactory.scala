package com.vinx.covid.statistics.data

object DataParserFactory {
  val metricsGeneratorCNMData : String = "italyDeclaredCasesOverTime"

  def createDataParser(metricsType : String, inputPath : String): DataParser ={
      metricsType match {
        case `metricsGeneratorCNMData` => new ItalianCNMDataParser(inputPath)
      }
  }
}
