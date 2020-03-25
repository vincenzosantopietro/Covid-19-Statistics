package com.vinx.covid.statistics.data
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers, TryValues}

class ItalianCNMDataParserSpec extends FlatSpec with Matchers with TryValues {

  behavior of "DataParser"

  it should "raise an exception with given wrong input path" in {
    val thrown = intercept[AnalysisException] {
      val dp = new ItalianCNMDataParser("not_existing_data.csv").getData
    }
    assert(thrown.getMessage().split(':')(0) === "Path does not exist")
  }
}
