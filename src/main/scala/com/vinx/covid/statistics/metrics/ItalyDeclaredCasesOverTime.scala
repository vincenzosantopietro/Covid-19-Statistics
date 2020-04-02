package com.vinx.covid.statistics.metrics
import com.pygmalios.reactiveinflux.Point.FieldKey
import com.pygmalios.reactiveinflux.{BigDecimalFieldValue, FieldValue, Point, StringFieldValue}
import com.vinx.covid.statistics.metrics.utils.InfluxDBSink
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
/***
 * Generates a plot containing the number of deaths in china over time
 *
 * @param:
 *    - data DataFrame with data
 *    - outputPath path where to store the output image
 */
class ItalyDeclaredCasesOverTime(data: DataFrame) extends MetricsGenerator {

  private val measurementName : String = "country-overall"

  def createFields(row: Row, schema: Array[String]): Map[FieldKey, FieldValue] = {
    var field = Map[FieldKey,FieldValue]()

    for(i <- 2 until schema.length){
      if(i == 2)
        field.+=((schema(i), StringFieldValue(row(i).toString)))
      else
        field.+=((schema(i), BigDecimalFieldValue(row(i).toString.toInt)))
    }
    field
  }

  def createPoints(dataList: List[Row], schema: Array[String]): ListBuffer[Point] = {
    var list = new ListBuffer[Point]()

    for(elem <- dataList){
      val field = createFields(elem, schema)
      list +=  Point(
        time = DateTime.parse(elem(0).toString.substring(0,10)),
        measurement = measurementName,
        tags = Map("Time"->"Infected"),
        fields = field
      )
    }
    list
  }

  override def generate(): Unit = {
    val schema = data.columns
    val dataList = data.collect().toList
    val points = createPoints(dataList,schema)

    new InfluxDBSink("http://localhost:8086/","covid-italy", measurementName, points).storeMetrics()
  }
}
