package com.vinx.covid.statistics.data

import java.io.FileNotFoundException

import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class MetricsCollectorSpec extends FlatSpec with Matchers {

  behavior of MetricsCollector.getClass.getSimpleName

  it should "return a list of 1 metrics" in {
    val metrics = MetricsCollector.collectMetrics("src/test/resources/config.json")
    metrics.size shouldBe 1
  }

  it should "raise an exception with wrong input" in {
    val thrown = intercept[FileNotFoundException] {
      val metrics = MetricsCollector.collectMetrics("not_existing_file.json")
    }
  }
}
