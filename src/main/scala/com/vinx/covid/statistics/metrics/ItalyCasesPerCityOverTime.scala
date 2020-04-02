package com.vinx.covid.statistics.metrics

import java.math.MathContext

import com.pygmalios.reactiveinflux.{BigDecimalFieldValue, FieldValue, Point, StringFieldValue}
import com.pygmalios.reactiveinflux.Point.FieldKey
import com.vinx.covid.statistics.metrics.utils.InfluxDBSink
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import java.math.MathContext

class ItalyCasesPerCityOverTime(data: DataFrame) extends MetricsGenerator {

  private val measurementName : String = "country-cities"

  def createFields(row: Row, schema: Array[String]): Map[FieldKey, FieldValue] = {
    var field = Map[FieldKey,FieldValue]()

    field.+=((schema(1),StringFieldValue(row(1).toString)))
    field.+=((schema(2),StringFieldValue(row(2).toString)))
    field.+=((schema(3),StringFieldValue(row(3).toString)))

    field.+=((schema(4),BigDecimalFieldValue(BigDecimal(row(4).toString.toDouble))))
    field.+=((schema(5),BigDecimalFieldValue(BigDecimal(row(5).toString.toDouble))))
    field.+=((schema(6),BigDecimalFieldValue(BigDecimal(row(6).toString.toDouble))))

    field
  }

  def createPoints(dataList: List[Row], schema: Array[String]): ListBuffer[Point] = {
    var list = new ListBuffer[Point]()
    val dataClean = dataList.drop(dataList.size-1)

    for(elem <- dataClean){
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
    //println(schema)
    val dataList = data.collect().toList
    //data.show()
    val points = createPoints(dataList,schema)

    new InfluxDBSink("http://localhost:8086/","covid-italy", measurementName, points).storeMetrics()
  }
}

