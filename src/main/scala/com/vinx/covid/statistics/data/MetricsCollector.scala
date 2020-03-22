package com.vinx.covid.statistics.data

import scala.io.Source
import scala.util.parsing.json.JSON
import net.liftweb.json._

import scala.collection.mutable.ListBuffer

object Get {
  def string(value: JValue): String = {
    val JString(result) = value
    result
  }
}

object MetricsCollector {

  def collectMetrics(jsonFilePath: String): List[String] = {
    var inputText = ""
    var mCollected = new ListBuffer[String]()

    val source = Source.fromFile(jsonFilePath)
    lazy val textJson = try source.mkString finally source.close()

    val data = JsonParser.parse(textJson)
    val metrics = (data \ "metrics").children

    for (elem <- metrics) {
      mCollected += Get.string(elem)
    }

    mCollected.toList
  }
}
