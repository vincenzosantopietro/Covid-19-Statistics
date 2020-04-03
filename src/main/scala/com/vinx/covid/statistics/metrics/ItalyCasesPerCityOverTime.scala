package com.vinx.covid.statistics.metrics

import java.math.MathContext

import com.pygmalios.reactiveinflux.{BigDecimalFieldValue, FieldValue, Point, StringFieldValue}
import com.pygmalios.reactiveinflux.Point.FieldKey
import com.vinx.covid.statistics.metrics.utils.InfluxDBSink
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import java.math.MathContext
import java.util.{Calendar, Date}

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark
import org.apache.spark.util.random

import scala.util.Random

class ItalyCasesPerCityOverTime(data: DataFrame) extends MetricsGenerator {

  private val measurementName : String = "country-cities"
  private val spark = SparkSession.builder().appName("covid_statistics").master("local[*]").getOrCreate()

  def createFields(row: Row, schema: Array[String]): Map[FieldKey, FieldValue] = {
    var field = Map[FieldKey,FieldValue]()

    field.+=((schema(0),StringFieldValue(row(0).toString)))
    field.+=((schema(1),BigDecimalFieldValue(BigDecimal(row(1).toString.toDouble))))
    field.+=((schema(2),BigDecimalFieldValue(BigDecimal(row(2).toString.toDouble))))
    field.+=((schema(3),BigDecimalFieldValue(BigDecimal(row(3).toString.toInt))))

    field
  }

  def createPoints(dataList: List[Row], schema: Array[String]): ListBuffer[Point] = {

    var list = new ListBuffer[Point]()
    for(elem <- dataList){
      val field = createFields(elem, schema)
      //println(s"field: ${field}")
      list +=  Point(
        time = new DateTime().withMillisOfSecond(Random.nextInt(999)), // we don't care time here
        measurement = measurementName,
        tags = Map("Time"->"Infected"),
        fields = field
      )
    }
    list
  }

  override def generate(): Unit = {

    val viewName : String = measurementName.split('-')(1)

    data.createOrReplaceTempView(viewName)
    val dateFilter = spark.sql(s"SELECT DISTINCT(MAX(date)) FROM ${viewName}").toDF("date").collect()
    val newData = data.filter(s"date >= cast(\'${dateFilter(0)(0)}\' as date)").selectExpr("city", "lat", "long", "cases")
    val schema = newData.columns
    val dataList = newData.collect().toList

    val points = createPoints(dataList,schema)
    points.foreach(println)
    new InfluxDBSink("http://localhost:8086/","covid-italy", measurementName, points).storeMetrics()
  }
}

